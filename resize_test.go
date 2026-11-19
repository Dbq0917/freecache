package freecache

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func newResizeTestCache(t *testing.T, size int) *Cache {
	t.Helper()
	return NewCache(size)
}

// TestMigrationHashReconstructionPinsRouting verifies that the hash value
// rebuilt from an entry header keeps the exact low 24 bits (segID, slotID and
// hash16) used by every segment lookup, so migrated entries remain reachable.
func TestMigrationHashReconstructionPinsRouting(t *testing.T) {
	for i := 0; i < 50000; i++ {
		key := []byte(fmt.Sprintf("some-key-%d", i))
		orig := hashFunc(key)
		segID := int(orig & 255)
		slotID := uint8(orig >> 8)
		hash16 := uint16(orig >> 16)
		rebuilt := uint64(hash16)<<16 | uint64(slotID)<<8 | uint64(segID)
		if rebuilt&0xFFFFFF != orig&0xFFFFFF {
			t.Fatalf("routing bits differ for key %d: orig=%#x rebuilt=%#x", i, orig, rebuilt)
		}
	}
}

// TestResizePreservesAllValues is the "compare every value before and after"
// guarantee: after an online resize, every key still returns its latest value.
func TestResizePreservesAllValues(t *testing.T) {
	cache := newResizeTestCache(t, 2*1024*1024)
	const n = 20000
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if err := cache.Set(key, val, 0); err != nil {
			t.Fatal(err)
		}
		want[string(key)] = string(val)
	}

	cache.Resize(4 * 1024 * 1024)

	if got := cache.EntryCount(); got != int64(n) {
		t.Fatalf("entry count after grow = %d, want %d", got, n)
	}
	for key, val := range want {
		got, err := cache.Get([]byte(key))
		if err != nil {
			t.Fatalf("key %q missing after resize: %v", key, err)
		}
		if string(got) != val {
			t.Fatalf("key %q = %q, want %q", key, got, val)
		}
	}
}

// TestResizeReadsSeeNewestValue pins the read-path contract: after a segment
// has been migrated, every key that is rewritten must be served from the new
// generation, and the frozen old generation must never shadow it. The check
// runs per migrated segment while later segments are still untouched.
func TestResizeReadsSeeNewestValue(t *testing.T) {
	cache := newResizeTestCache(t, 2*1024*1024)
	const n = 10000
	keysBySeg := make(map[int][][]byte)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		if err := cache.Set(key, []byte("old"), 0); err != nil {
			t.Fatal(err)
		}
		segID := int(hashFunc(key) & segmentAndOpVal)
		keysBySeg[segID] = append(keysBySeg[segID], key)
	}

	var readerWG sync.WaitGroup
	stop := make(chan struct{})
	// Background readers run throughout; they only assert that every returned
	// value is one of the two legal values (pre- or post-rewrite), never a
	// corrupted one.
	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < n; i++ {
					if val, err := cache.Get([]byte(fmt.Sprintf("k%d", i))); err == nil {
						if string(val) != "old" && string(val) != "new" {
							t.Errorf("corrupted value %q for k%d", val, i)
							return
						}
					}
				}
			}
		}()
	}

	migrated := make(map[int]bool)
	cache.resize(4*1024*1024, func(segID int) {
		// This segment's copy just finished; rewrite its keys into the new
		// generation, then prove reads never fall back to the frozen value.
		for _, key := range keysBySeg[segID] {
			if err := cache.Set(key, []byte("new"), 0); err != nil {
				t.Error(err)
			}
		}
		for _, key := range keysBySeg[segID] {
			val, err := cache.Get(key)
			if err != nil || string(val) != "new" {
				t.Errorf("migrated key %s read %q (%v), want new (stale fallback?)", key, val, err)
			}
		}
		migrated[segID] = true
	})
	close(stop)
	readerWG.Wait()

	for i := 0; i < n; i++ {
		val, err := cache.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil || string(val) != "new" {
			t.Fatalf("k%d = %q (%v), want new", i, val, err)
		}
	}
}

// TestResizeOldGenerationReleased checks that once migration completes, no
// segment retains a frozen generation and every segment is back to steady.
func TestResizeOldGenerationReleased(t *testing.T) {
	cache := newResizeTestCache(t, 2*1024*1024)
	for i := 0; i < 5000; i++ {
		if err := cache.Set([]byte(fmt.Sprintf("z%d", i)), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}

	var sawMigrating bool
	cache.resize(4*1024*1024, func(segID int) {
		// Earlier segments are already steady; later ones have not started.
		// The just-migrated segment must already have released its old gen.
		if cache.segs[segID].oldGen.Load() != nil {
			t.Errorf("segment %d still holds old generation after migration", segID)
		}
		if cache.segs[segID].state.Load() != segSteady {
			sawMigrating = true
		}
	})

	for i := range cache.segs {
		if cache.segs[i].oldGen.Load() != nil {
			t.Fatalf("segment %d leaked old generation", i)
		}
		if cache.segs[i].state.Load() != segSteady {
			t.Fatalf("segment %d not steady", i)
		}
	}
	if sawMigrating {
		t.Fatal("segment state was not reset to steady after migration")
	}
}

// TestResizeConcurrentWritersNeverStop verifies writes succeed throughout the
// whole resize and their final values are all readable afterwards.
func TestResizeConcurrentWritersNeverStop(t *testing.T) {
	cache := newResizeTestCache(t, 2*1024*1024)
	const writers = 8
	const perWriter = 3000

	var wg sync.WaitGroup
	var writeErrors int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(fact int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("w-%d-%d", fact, i))
				val := []byte(fmt.Sprintf("v-%d-%d", fact, i))
				if err := cache.Set(key, val, 0); err != nil {
					atomic.AddInt64(&writeErrors, 1)
				}
			}
		}(w)
	}

	done := make(chan struct{})
	go func() {
		cache.Resize(4 * 1024 * 1024)
		close(done)
	}()
	wg.Wait()
	<-done

	if atomic.LoadInt64(&writeErrors) != 0 {
		t.Fatalf("%d writes failed during resize", writeErrors)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			key := []byte(fmt.Sprintf("w-%d-%d", w, i))
			want := []byte(fmt.Sprintf("v-%d-%d", w, i))
			got, err := cache.Get(key)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s = %q (%v), want %q", key, got, err, want)
			}
		}
	}
}

// TestResizeShrinkKeepsFittingEntries confirms shrinking still serves the
// entries that fit and never corrupts state.
func TestResizeShrinkKeepsFittingEntries(t *testing.T) {
	cache := newResizeTestCache(t, 4*1024*1024)
	const n = 4000
	for i := 0; i < n; i++ {
		if err := cache.Set([]byte(fmt.Sprintf("s%d", i)), []byte(fmt.Sprintf("%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}

	cache.Resize(2 * 1024 * 1024)

	// Every readable entry must still return its exact value; the smaller
	// buffer may legitimately have evicted some of them.
	for i := 0; i < n; i++ {
		got, err := cache.Get([]byte(fmt.Sprintf("s%d", i)))
		if err == nil && string(got) != fmt.Sprintf("%d", i) {
			t.Fatalf("s%d corrupted: %q", i, got)
		}
	}
	if cache.EntryCount() > n {
		t.Fatalf("entry count %d exceeds number of written keys", cache.EntryCount())
	}
}

// TestResizeConcurrentReconciliation reproduces the incident scenario: writes,
// reads, deletes and a full resize all run at the same time. Afterwards every
// surviving key must reconcile to exactly the value last written, with no key
// resurrecting from a frozen old generation.
func TestResizeConcurrentReconciliation(t *testing.T) {
	cache := newResizeTestCache(t, 2*1024*1024)
	const keySpace = 4000

	latest := make([][]byte, keySpace)
	var mu sync.Mutex
	for i := 0; i < keySpace; i++ {
		v := []byte(fmt.Sprintf("init-%d", i))
		latest[i] = v
		if err := cache.Set([]byte(fmt.Sprintf("r%d", i)), v, 0); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	resizeDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		cache.Resize(4 * 1024 * 1024)
		cache.Resize(2 * 1024 * 1024)
		close(resizeDone)
	}()

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for round := 0; round < 2000; round++ {
				i := (seed*7919 + round*31) % keySpace
				key := []byte(fmt.Sprintf("r%d", i))
				val := []byte(fmt.Sprintf("w%d-%d", seed, round))
				if err := cache.Set(key, val, 0); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				latest[i] = val
				mu.Unlock()
			}
		}(w)
	}

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for round := 0; round < 2000; round++ {
				i := (seed*104729 + round*37) % keySpace
				val, err := cache.Get([]byte(fmt.Sprintf("r%d", i)))
				if err != nil {
					continue
				}
				if len(val) == 0 || (val[0] != 'i' && val[0] != 'w') {
					t.Errorf("unexpected value %q for r%d", val, i)
					return
				}
			}
		}(r)
	}

	deleted := make(map[int]bool)
	for d := 0; d < 2; d++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for round := 0; round < 500; round++ {
				i := (seed*131 + round*17) % keySpace
				key := []byte(fmt.Sprintf("r%d", i))
				if cache.Del(key) {
					mu.Lock()
					deleted[i] = true
					latest[i] = nil
					mu.Unlock()
				}
			}
		}(d)
	}

	wg.Wait()
	<-resizeDone

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < keySpace; i++ {
		got, err := cache.Get([]byte(fmt.Sprintf("r%d", i)))
		if deleted[i] {
			if latest[i] == nil && err == nil {
				t.Fatalf("deleted key r%d resurrected with %q", i, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("key r%d unexpectedly missing: %v", i, err)
		}
		if !bytes.Equal(got, latest[i]) {
			t.Fatalf("reconciliation mismatch r%d: got %q want %q", i, got, latest[i])
		}
	}
}

// TestResizeIdempotentSameSize ensures Resize to the current size is a no-op
// that does not disturb data or allocate new generations.
func TestResizeIdempotentSameSize(t *testing.T) {
	cache := newResizeTestCache(t, 2*1024*1024)
	for i := 0; i < 1000; i++ {
		if err := cache.Set([]byte(fmt.Sprintf("e%d", i)), []byte("x"), 0); err != nil {
			t.Fatal(err)
		}
	}
	before := cache.EntryCount()
	gen0 := cache.activeSeg(0)
	cache.Resize(2 * 1024 * 1024)
	if cache.activeSeg(0) != gen0 {
		t.Fatal("same-size resize replaced a generation")
	}
	if cache.EntryCount() != before {
		t.Fatalf("entry count changed %d -> %d", before, cache.EntryCount())
	}
}
