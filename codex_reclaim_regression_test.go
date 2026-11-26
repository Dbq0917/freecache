package freecache

import (
	"bytes"
	"testing"
)

func codexReclaimEntryOffset(t *testing.T, seg *segment, key []byte) int64 {
	t.Helper()
	hashVal := hashFunc(key)
	slotID := uint8(hashVal >> 8)
	slot := seg.getSlot(slotID)
	idx, matched := seg.lookup(slot, uint16(hashVal>>16), key)
	if !matched {
		t.Fatalf("key %q not found", key)
	}
	return slot[idx].offset
}

func TestCodexReclaimDeletedRegionImmediately(t *testing.T) {
	seg := newSegment(1024, 0, defaultTimer{})
	guardKey := []byte("guard")
	oldKey := []byte("old-key")
	newKey := []byte("new-key")
	oldValue := make([]byte, 100)
	newValue := []byte("small")

	if err := seg.set(guardKey, []byte("stay"), hashFunc(guardKey), 0); err != nil {
		t.Fatal(err)
	}
	if err := seg.set(oldKey, oldValue, hashFunc(oldKey), 0); err != nil {
		t.Fatal(err)
	}
	oldOff := codexReclaimEntryOffset(t, &seg, oldKey)
	endAfterSet := seg.rb.End()

	if !seg.del(oldKey, hashFunc(oldKey)) {
		t.Fatal("expected delete to find the entry")
	}
	if len(seg.freeRegions) != 1 {
		t.Fatalf("free regions = %d, want 1", len(seg.freeRegions))
	}

	if err := seg.set(newKey, newValue, hashFunc(newKey), 0); err != nil {
		t.Fatal(err)
	}
	if got := codexReclaimEntryOffset(t, &seg, newKey); got != oldOff {
		t.Fatalf("new entry offset = %d, want reused offset %d", got, oldOff)
	}
	if got := seg.rb.End(); got != endAfterSet {
		t.Fatalf("ring end = %d, want unchanged %d", got, endAfterSet)
	}
	got, _, err := seg.get(newKey, nil, hashFunc(newKey), false)
	if err != nil || !bytes.Equal(got, newValue) {
		t.Fatalf("Get after region reuse = %q, %v", got, err)
	}
}

func TestCodexFrontierDeleteReturnsVacuumImmediately(t *testing.T) {
	seg := newSegment(1024, 0, defaultTimer{})
	key := []byte("frontier")
	value := make([]byte, 64)
	hashVal := hashFunc(key)
	if err := seg.set(key, value, hashVal, 0); err != nil {
		t.Fatal(err)
	}
	off := codexReclaimEntryOffset(t, &seg, key)
	vacuumBefore := seg.vacuumLen
	wantReclaimed := int64(ENTRY_HDR_SIZE + len(key) + len(value))

	if !seg.del(key, hashVal) {
		t.Fatal("expected delete to find the entry")
	}
	if got := seg.vacuumLen - vacuumBefore; got != wantReclaimed {
		t.Fatalf("vacuum reclaimed = %d, want %d", got, wantReclaimed)
	}
	if len(seg.freeRegions) != 0 {
		t.Fatalf("frontier region should be accounted directly, got %d free regions", len(seg.freeRegions))
	}
	if seg.totalCount != 0 {
		t.Fatalf("totalCount after frontier delete = %d, want 0", seg.totalCount)
	}
	if off != 0 {
		t.Fatalf("test setup offset = %d, want frontier offset 0", off)
	}
}

func TestCodexReclaimGrowingEntryExpandsTailInPlace(t *testing.T) {
	seg := newSegment(4096, 0, defaultTimer{})
	key := []byte("hot")
	hashVal := hashFunc(key)

	values := [][]byte{
		make([]byte, 8),
		make([]byte, 16),
		make([]byte, 32),
	}
	for i, value := range values {
		if err := seg.set(key, value, hashVal, 0); err != nil {
			t.Fatal(err)
		}
		got, _, err := seg.get(key, nil, hashVal, true)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("Get after set %d = len %d, %v", i, len(got), err)
		}
	}

	if off := codexReclaimEntryOffset(t, &seg, key); off != 0 {
		t.Fatalf("latest entry offset = %d, want stable tail offset 0", off)
	}
	wantEnd := int64(ENTRY_HDR_SIZE + len(key) + 32)
	if seg.rb.End() != wantEnd {
		t.Fatalf("ring end = %d, want latest entry end %d", seg.rb.End(), wantEnd)
	}
	if seg.totalCount != 1 {
		t.Fatalf("total ring records = %d, want 1", seg.totalCount)
	}
}
