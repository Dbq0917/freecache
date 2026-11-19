package freecache

import (
	"sync/atomic"
	"unsafe"
)

// Resize changes the total cache capacity at runtime while the cache keeps
// serving reads and writes. It migrates one segment at a time:
//
//  1. The segment being migrated is locked only for the duration of its copy.
//     Every other segment stays fully available, and once the copy completes
//     the migrated segment immediately accepts writes again.
//  2. During the copy the segment briefly has two generations: the frozen old
//     one and the active new one. Reads consult the new generation first and
//     fall back to the old one, so a key copied early but updated later is
//     always read from the generation that holds its latest value.
//  3. When the copy finishes, the old generation is detached and its ring
//     buffer is released; there is never a permanently half-migrated segment.
//
// Shrink requests discard the least recently used entries that no longer fit,
// matching the cache's normal eviction policy.
func (cache *Cache) Resize(newSize int) {
	cache.resize(newSize, nil)
}

// resize performs the per-segment migration. afterSegment, when non-nil, is
// invoked after each segment is migrated while no segment lock is held,
// allowing tests to observe the cache mid-migration.
func (cache *Cache) resize(newSize int, afterSegment func(segID int)) {
	if newSize < minBufSize {
		newSize = minBufSize
	}
	newSegSize := newSize / segmentCount
	for segID := 0; segID < segmentCount; segID++ {
		cache.migrateSegment(segID, newSegSize)
		if afterSegment != nil {
			afterSegment(segID)
		}
	}
}

// migrateSegment moves every live entry of segment segID into a freshly
// allocated segment backed by a ring buffer of newSegSize bytes.
func (cache *Cache) migrateSegment(segID, newSegSize int) {
	lock := &cache.locks[segID]
	holder := &cache.segs[segID]

	// The write lock blocks writers and readers of this segment only while the
	// snapshot is taken; all other segments keep operating normally.
	lock.Lock()
	oldSeg := holder.gen.Load()
	if len(oldSeg.rb.data) == newSegSize {
		lock.Unlock()
		return
	}

	newSeg := newSegment(newSegSize, segID, oldSeg.timer)
	holder.oldGen.Store(oldSeg)
	holder.state.Store(segMigrating)

	// Copy every live entry under the same lock. Writes cannot interleave with
	// this snapshot, so the new generation starts as an exact copy of the old
	// one; subsequent writes go to the new generation only.
	for _, entry := range snapshotSegment(oldSeg) {
		// A failed set on shrink (entry too large for the smaller buffer) is
		// expected; the entry is simply dropped as an eviction would do.
		_ = newSeg.setWithExpireAt(entry.key, entry.value, entry.hashVal, entry.expireAt)
	}

	// Carry over the cumulative debug counters so metrics do not jump backwards
	// after a resize. LRU bookkeeping starts fresh in the new ring buffer.
	newSeg.totalEvacuate = atomic.LoadInt64(&oldSeg.totalEvacuate)
	newSeg.totalExpired = atomic.LoadInt64(&oldSeg.totalExpired)
	newSeg.hitCount = atomic.LoadInt64(&oldSeg.hitCount)
	newSeg.missCount = atomic.LoadInt64(&oldSeg.missCount)
	newSeg.overwrites = atomic.LoadInt64(&oldSeg.overwrites)
	newSeg.touched = atomic.LoadInt64(&oldSeg.touched)

	holder.gen.Store(&newSeg)
	holder.oldGen.Store(nil)
	holder.state.Store(segSteady)
	lock.Unlock()
}

// migratableEntry is the materialized view of one live segment entry.
type migratableEntry struct {
	key      []byte
	value    []byte
	expireAt uint32
	hashVal  uint64
}

// snapshotSegment materializes the live entries of a segment. The caller must
// hold the segment write lock, which guarantees the ring buffer and slot index
// are stable during the walk.
func snapshotSegment(seg *segment) []migratableEntry {
	now := seg.timer.Now()
	entries := make([]migratableEntry, 0, atomic.LoadInt64(&seg.entryCount))
	for slotID := 0; slotID < 256; slotID++ {
		slotOff := int32(slotID) * seg.slotCap
		slot := seg.slotsData[slotOff : slotOff+seg.slotLens[slotID] : slotOff+seg.slotCap]
		for ptrIdx := range slot {
			entry, ok := readLiveEntry(seg, slot[ptrIdx].offset, now)
			if !ok {
				continue
			}
			entries = append(entries, entry)
		}
	}
	return entries
}

// readLiveEntry reconstructs a migratable entry from the ring buffer. It
// returns ok=false for entries that have already expired, mirroring the
// iterator's behavior.
func readLiveEntry(seg *segment, offset int64, now uint32) (entry migratableEntry, ok bool) {
	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if isExpired(hdr.expireAt, now) {
		return migratableEntry{}, false
	}

	key := make([]byte, hdr.keyLen)
	seg.rb.ReadAt(key, offset+ENTRY_HDR_SIZE)
	value := make([]byte, hdr.valLen)
	seg.rb.ReadAt(value, offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))

	hashVal := uint64(hdr.hash16)<<16 | uint64(hdr.slotId)<<8 | uint64(seg.segId)
	return migratableEntry{
		key:      key,
		value:    value,
		expireAt: hdr.expireAt,
		hashVal:  hashVal,
	}, true
}
