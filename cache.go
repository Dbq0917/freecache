package freecache

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const (
	// segmentCount represents the number of segments within a freecache instance.
	segmentCount = 256
	// segmentAndOpVal is bitwise AND applied to the hashVal to find the segment id.
	segmentAndOpVal = 255
	minBufSize      = 512 * 1024
	// minSegmentSize is the smallest ring buffer a segment may be shrunk to.
	minSegmentSize = 1024
	// rebalanceCheckInterval is the number of cache accesses between rebalance
	// checks. It is a power of two so the check is a cheap mask.
	rebalanceCheckInterval = 64
	// rebalanceMinHotAccess avoids treating a few random accesses in a short
	// window as a hotspot.
	rebalanceMinHotAccess = 16
	// rebalanceHotRatio controls how skewed a window must be before capacity is
	// moved. The hottest segment must receive at least ratio times its fair
	// share of the accesses in the window.
	rebalanceHotRatio = 8
)

// Cache is a freecache instance.
type Cache struct {
	locks    [segmentCount]sync.Mutex
	segments [segmentCount]segment

	totalSize int64

	rebalanceMu   sync.Mutex
	lastAccess    [segmentCount]int64
	opCount       int64
	nextRebalance int64
}

type Updater func(value []byte, found bool) (newValue []byte, replace bool, expireSeconds int)

func hashFunc(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// NewCache returns a newly initialize cache by size.
// The cache size will be set to 512KB at minimum.
// If the size is set relatively large, you should call
// `debug.SetGCPercent()`, set it to a much smaller value
// to limit the memory consumption and GC pause time.
func NewCache(size int) (cache *Cache) {
	return NewCacheCustomTimer(size, defaultTimer{})
}

// NewCacheCustomTimer returns new cache with custom timer.
func NewCacheCustomTimer(size int, timer Timer) (cache *Cache) {
	if size < minBufSize {
		size = minBufSize
	}
	if timer == nil {
		timer = defaultTimer{}
	}
	cache = new(Cache)
	baseSize := size / segmentCount
	remainder := size % segmentCount
	totalSize := 0
	for i := 0; i < segmentCount; i++ {
		segSize := baseSize
		if i < remainder {
			segSize++
		}
		cache.segments[i] = newSegment(segSize, i, timer)
		totalSize += segSize
	}
	cache.totalSize = int64(totalSize)
	cache.nextRebalance = rebalanceCheckInterval
	return
}

// segmentIndex is the single source of truth for mapping a key hash to a
// segment. Keeping this in one place is important: every cache operation must
// agree on the segment while capacity is being moved between segments.
func (cache *Cache) segmentIndex(hashVal uint64) int {
	return int(hashVal & segmentAndOpVal)
}

// segmentForHash resolves a hash to its stable segment and records the access
// used by the load balancer.
func (cache *Cache) segmentForHash(hashVal uint64) (segID int, seg *segment) {
	segID = cache.segmentIndex(hashVal)
	seg = &cache.segments[segID]
	cache.noteAccess(segID)
	return segID, seg
}

func (cache *Cache) noteAccess(segID int) {
	atomic.AddInt64(&cache.segments[segID].accessCount, 1)
	atomic.AddInt64(&cache.opCount, 1)
}

// maybeRebalance is called after segment locks are released, so a callback
// running inside a cache operation can safely use the cache.
func (cache *Cache) maybeRebalance() {
	n := atomic.LoadInt64(&cache.opCount)
	next := atomic.LoadInt64(&cache.nextRebalance)
	if next == 0 {
		atomic.CompareAndSwapInt64(&cache.nextRebalance, 0, rebalanceCheckInterval)
		return
	}
	if n >= next && atomic.CompareAndSwapInt64(&cache.nextRebalance, next, n+rebalanceCheckInterval) {
		cache.rebalance()
	}
}

// rebalance samples access counts and, when the window is sufficiently skewed,
// moves ring-buffer capacity from cold segments to hot ones. The total capacity
// is conserved and a segment is never shrunk below the bytes it already uses.
func (cache *Cache) rebalance() {
	cache.rebalanceMu.Lock()
	defer cache.rebalanceMu.Unlock()

	var deltas [segmentCount]int64
	var total, maxDelta int64
	hottest := 0
	for i := 0; i < segmentCount; i++ {
		cur := atomic.LoadInt64(&cache.segments[i].accessCount)
		delta := cur - cache.lastAccess[i]
		if delta < 0 {
			delta = 0
		}
		deltas[i] = delta
		total += delta
		if delta > maxDelta {
			maxDelta = delta
			hottest = i
		}
	}
	if total < rebalanceCheckInterval {
		return
	}
	for i := 0; i < segmentCount; i++ {
		cache.lastAccess[i] += deltas[i]
	}
	if maxDelta < rebalanceMinHotAccess {
		return
	}
	if maxDelta*segmentCount < total*rebalanceHotRatio {
		return
	}
	cache.redistribute(deltas, hottest)
}

func (cache *Cache) redistribute(deltas [segmentCount]int64, hottest int) {
	for i := 0; i < segmentCount; i++ {
		cache.locks[i].Lock()
	}
	defer func() {
		for i := segmentCount - 1; i >= 0; i-- {
			cache.locks[i].Unlock()
		}
	}()

	var caps, used, lower, weights, desired [segmentCount]int64
	var totalCap, sumLower, weightSum int64

	minSize := cache.totalSize / segmentCount / 2
	if minSize < minSegmentSize {
		minSize = minSegmentSize
	}
	for i := 0; i < segmentCount; i++ {
		capSize := int64(cache.segments[i].rb.Size())
		caps[i] = capSize
		totalCap += capSize

		usedSize := cache.segments[i].rb.End() - cache.segments[i].rb.Begin()
		if usedSize < 0 {
			usedSize = 0
		}
		if usedSize > capSize {
			usedSize = capSize
		}
		used[i] = usedSize

		floor := usedSize
		if floor < minSize {
			floor = minSize
		}
		lower[i] = floor
		sumLower += floor

		weight := deltas[i]
		weights[i] = weight
		weightSum += weight
	}

	if sumLower > totalCap {
		sumLower = 0
		for i := 0; i < segmentCount; i++ {
			lower[i] = used[i]
			sumLower += used[i]
		}
	}
	leftover := totalCap - sumLower
	if leftover < 0 {
		leftover = 0
	}
	if weightSum == 0 {
		weightSum = 1
	}

	var assigned int64
	for i := 0; i < segmentCount; i++ {
		share := leftover * weights[i] / weightSum
		desired[i] = lower[i] + share
		assigned += share
	}
	for remaining := leftover - assigned; remaining > 0; remaining-- {
		idx := (hottest + int(remaining)) % segmentCount
		desired[idx]++
	}

	for i := 0; i < segmentCount; i++ {
		newSize := desired[i]
		if newSize < 1 {
			newSize = 1
		}
		if newSize == caps[i] {
			continue
		}
		cache.segments[i].rb.Resize(int(newSize))
		cache.segments[i].vacuumLen = newSize - used[i]
	}
}

// Set sets a key, value and expiration for a cache entry and stores it in the cache.
// If the key is larger than 65535 or value is larger than 1/1024 of the cache size,
// the entry will not be written to the cache. expireSeconds <= 0 means no expire,
// but it can be evicted when cache is full.
func (cache *Cache) Set(key, value []byte, expireSeconds int) (err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	err = seg.set(key, value, hashVal, expireSeconds)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// Touch updates the expiration time of an existing key. expireSeconds <= 0 means no expire,
// but it can be evicted when cache is full.
func (cache *Cache) Touch(key []byte, expireSeconds int) (err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	err = seg.touch(key, hashVal, expireSeconds)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// Get returns the value or not found error.
func (cache *Cache) Get(key []byte) (value []byte, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	value, _, err = seg.get(key, nil, hashVal, false)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// MultiGet returns values and errors for the given keys. The returned slices
// have the same length as keys; values[i] and errs[i] correspond to keys[i].
// A miss is represented by values[i] == nil and errs[i] == ErrNotFound.
// MultiGet reduces lock contention by grouping keys by segment and acquiring
// each segment lock at most once.
// Note that MultiGet holds each segment lock longer than a single Get (for
// the duration of all keys in that segment), which can increase Get tail
// latency when MultiGet and Get run concurrently.
func (cache *Cache) MultiGet(keys [][]byte) (values [][]byte, errs []error) {
	n := len(keys)
	if n == 0 {
		return nil, nil
	}
	values = make([][]byte, n)
	errs = make([]error, n)
	type keyLoc struct {
		idx     int
		hashVal uint64
	}
	var groups [segmentCount][]keyLoc
	for i, key := range keys {
		hashVal := hashFunc(key)
		segID := cache.segmentIndex(hashVal)
		cache.noteAccess(segID)
		groups[segID] = append(groups[segID], keyLoc{idx: i, hashVal: hashVal})
	}
	for segID := 0; segID < segmentCount; segID++ {
		batch := groups[segID]
		if len(batch) == 0 {
			continue
		}
		cache.locks[segID].Lock()
		for _, loc := range batch {
			value, _, err := cache.segments[segID].get(keys[loc.idx], nil, loc.hashVal, false)
			values[loc.idx] = value
			errs[loc.idx] = err
		}
		cache.locks[segID].Unlock()
	}
	cache.maybeRebalance()
	return values, errs
}

// GetFn is equivalent to Get or GetWithBuf, but it attempts to be zero-copy,
// calling the provided function with slice view over the current underlying
// value of the key in memory. The slice is constrained in length and capacity.
//
// In moth cases, this method will not alloc a byte buffer. The only exception
// is when the value wraps around the underlying segment ring buffer.
//
// The method will return ErrNotFound is there's a miss, and the function will
// not be called. Errors returned by the function will be propagated.
func (cache *Cache) GetFn(key []byte, fn func([]byte) error) (err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	defer func() {
		cache.locks[segID].Unlock()
		cache.maybeRebalance()
	}()
	err = seg.view(key, fn, hashVal, false)
	return
}

// GetOrSet returns existing value or if record doesn't exist
// it sets a new key, value and expiration for a cache entry and stores it in the cache, returns nil in that case
func (cache *Cache) GetOrSet(key, value []byte, expireSeconds int) (retValue []byte, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	defer func() {
		cache.locks[segID].Unlock()
		cache.maybeRebalance()
	}()

	retValue, _, err = seg.get(key, nil, hashVal, false)
	if err != nil {
		err = seg.set(key, value, hashVal, expireSeconds)
	}
	return
}

// SetAndGet sets a key, value and expiration for a cache entry and stores it in the cache.
// If the key is larger than 65535 or value is larger than 1/1024 of the cache size,
// the entry will not be written to the cache. expireSeconds <= 0 means no expire,
// but it can be evicted when cache is full.  Returns existing value if record exists
// with a bool value to indicate whether an existing record was found
func (cache *Cache) SetAndGet(key, value []byte, expireSeconds int) (retValue []byte, found bool, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	defer func() {
		cache.locks[segID].Unlock()
		cache.maybeRebalance()
	}()

	retValue, _, err = seg.get(key, nil, hashVal, false)
	if err == nil {
		found = true
	}
	err = seg.set(key, value, hashVal, expireSeconds)
	return
}

// Update gets value for a key, passes it to updater function that decides if set should be called as well
// This allows for an atomic Get plus Set call using the existing value to decide on whether to call Set.
// If the key is larger than 65535 or value is larger than 1/1024 of the cache size,
// the entry will not be written to the cache. expireSeconds <= 0 means no expire,
// but it can be evicted when cache is full. Returns bool value to indicate if existing record was found along with bool
// value indicating the value was replaced and error if any
func (cache *Cache) Update(key []byte, updater Updater) (found bool, replaced bool, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	defer func() {
		cache.locks[segID].Unlock()
		cache.maybeRebalance()
	}()

	retValue, _, err := seg.get(key, nil, hashVal, false)
	if err == nil {
		found = true
	} else {
		err = nil // Clear ErrNotFound error since we're returning found flag
	}
	value, replaced, expireSeconds := updater(retValue, found)
	if !replaced {
		return
	}
	err = seg.set(key, value, hashVal, expireSeconds)
	return
}

// Peek returns the value or not found error, without updating access time or counters.
// Warning: No expiry check is performed so if an expired value is found, it will be
// returned without error
func (cache *Cache) Peek(key []byte) (value []byte, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	value, _, err = seg.get(key, nil, hashVal, true)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// PeekWithExpiration returns the value and expiration time, without updating access time or counters.
// Warning: No expiry check is performed so if an expired value is found, it will be
// returned without error
func (cache *Cache) PeekWithExpiration(key []byte) (value []byte, expireAt uint32, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	value, expireAt, err = seg.get(key, nil, hashVal, true)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// PeekFn is equivalent to Peek, but it attempts to be zero-copy, calling the
// provided function with slice view over the current underlying value of the
// key in memory. The slice is constrained in length and capacity.
//
// In most cases, this method will not alloc a byte buffer. The only exception
// is when the value wraps around the underlying segment ring buffer.
//
// The method will return ErrNotFound is there's a miss, and the function will
// not be called. Errors returned by the function will be propagated.
// Warning: No expiry check is performed so if an expired value is found, it will be
// returned without error
func (cache *Cache) PeekFn(key []byte, fn func([]byte) error) (err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	defer func() {
		cache.locks[segID].Unlock()
		cache.maybeRebalance()
	}()
	err = seg.view(key, fn, hashVal, true)
	return
}

// GetWithBuf copies the value to the buf or returns not found error.
// This method doesn't allocate memory when the capacity of buf is greater or equal to value.
func (cache *Cache) GetWithBuf(key, buf []byte) (value []byte, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	value, _, err = seg.get(key, buf, hashVal, false)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// GetWithExpiration returns the value with expiration or not found error.
func (cache *Cache) GetWithExpiration(key []byte) (value []byte, expireAt uint32, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	value, expireAt, err = seg.get(key, nil, hashVal, false)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// GetWithExpirationAndBuf copies the value to the buf and gets with expiration or returns a not found error.
// This method doesn't allocate memory when the capacity of buf is greater or equal to value.
func (cache *Cache) GetWithExpirationAndBuf(key []byte, buf []byte) (value []byte, expireAt uint32, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	value, expireAt, err = seg.get(key, buf, hashVal, false)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// TTL returns the TTL time left for a given key or a not found error.
func (cache *Cache) TTL(key []byte) (timeLeft uint32, err error) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	timeLeft, err = seg.ttl(key, hashVal)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// Del deletes an item in the cache by key and returns true or false if a delete occurred.
func (cache *Cache) Del(key []byte) (affected bool) {
	hashVal := hashFunc(key)
	segID, seg := cache.segmentForHash(hashVal)
	cache.locks[segID].Lock()
	affected = seg.del(key, hashVal)
	cache.locks[segID].Unlock()
	cache.maybeRebalance()
	return
}

// SetInt stores in integer value in the cache.
func (cache *Cache) SetInt(key int64, value []byte, expireSeconds int) (err error) {
	var bKey [8]byte
	binary.LittleEndian.PutUint64(bKey[:], uint64(key))
	return cache.Set(bKey[:], value, expireSeconds)
}

// GetInt returns the value for an integer within the cache or a not found error.
func (cache *Cache) GetInt(key int64) (value []byte, err error) {
	var bKey [8]byte
	binary.LittleEndian.PutUint64(bKey[:], uint64(key))
	return cache.Get(bKey[:])
}

// GetIntWithExpiration returns the value and expiration or a not found error.
func (cache *Cache) GetIntWithExpiration(key int64) (value []byte, expireAt uint32, err error) {
	var bKey [8]byte
	binary.LittleEndian.PutUint64(bKey[:], uint64(key))
	return cache.GetWithExpiration(bKey[:])
}

// DelInt deletes an item in the cache by int key and returns true or false if a delete occurred.
func (cache *Cache) DelInt(key int64) (affected bool) {
	var bKey [8]byte
	binary.LittleEndian.PutUint64(bKey[:], uint64(key))
	return cache.Del(bKey[:])
}

// EvacuateCount is a metric indicating the number of times an eviction occurred.
func (cache *Cache) EvacuateCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalEvacuate)
	}
	return
}

// ExpiredCount is a metric indicating the number of times an expire occurred.
func (cache *Cache) ExpiredCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalExpired)
	}
	return
}

// EntryCount returns the number of items currently in the cache.
func (cache *Cache) EntryCount() (entryCount int64) {
	for i := range cache.segments {
		entryCount += atomic.LoadInt64(&cache.segments[i].entryCount)
	}
	return
}

// AverageAccessTime returns the average unix timestamp when a entry being accessed.
// Entries have greater access time will be evacuated when it
// is about to be overwritten by new value.
func (cache *Cache) AverageAccessTime() int64 {
	var entryCount, totalTime int64
	for i := range cache.segments {
		totalTime += atomic.LoadInt64(&cache.segments[i].totalTime)
		entryCount += atomic.LoadInt64(&cache.segments[i].totalCount)
	}
	if entryCount == 0 {
		return 0
	} else {
		return totalTime / entryCount
	}
}

// HitCount is a metric that returns number of times a key was found in the cache.
func (cache *Cache) HitCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].hitCount)
	}
	return
}

// MissCount is a metric that returns the number of times a miss occurred in the cache.
func (cache *Cache) MissCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].missCount)
	}
	return
}

// LookupCount is a metric that returns the number of times a lookup for a given key occurred.
func (cache *Cache) LookupCount() int64 {
	return cache.HitCount() + cache.MissCount()
}

// HitRate is the ratio of hits over lookups.
func (cache *Cache) HitRate() float64 {
	hitCount, missCount := cache.HitCount(), cache.MissCount()
	lookupCount := hitCount + missCount
	if lookupCount == 0 {
		return 0
	} else {
		return float64(hitCount) / float64(lookupCount)
	}
}

// OverwriteCount indicates the number of times entries have been overridden.
func (cache *Cache) OverwriteCount() (overwriteCount int64) {
	for i := range cache.segments {
		overwriteCount += atomic.LoadInt64(&cache.segments[i].overwrites)
	}
	return
}

// TouchedCount indicates the number of times entries have had their expiration time extended.
func (cache *Cache) TouchedCount() (touchedCount int64) {
	for i := range cache.segments {
		touchedCount += atomic.LoadInt64(&cache.segments[i].touched)
	}
	return
}

// Clear clears the cache.
func (cache *Cache) Clear() {
	for i := range cache.segments {
		cache.locks[i].Lock()
		cache.segments[i].clear()
		cache.locks[i].Unlock()
	}
}

// ResetStatistics refreshes the current state of the statistics.
func (cache *Cache) ResetStatistics() {
	for i := range cache.segments {
		cache.locks[i].Lock()
		cache.segments[i].resetStatistics()
		cache.locks[i].Unlock()
	}
}
