package freecache

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestCodexKeyLayoutSize(t *testing.T) {
	if ENTRY_HDR_SIZE != 24 {
		t.Fatalf("ENTRY_HDR_SIZE = %d, want 24", ENTRY_HDR_SIZE)
	}
	if got := unsafe.Sizeof(entryHdr{}); got != 24 {
		t.Fatalf("entryHdr size = %d, want 24", got)
	}
	if off := unsafe.Offsetof(entryHdr{}.keyLen); off != 8 {
		t.Fatalf("entryHdr.keyLen offset = %d, want 8", off)
	}
	if off := unsafe.Offsetof(entryHdr{}.reserved); off != 22 {
		t.Fatalf("entryHdr.reserved offset = %d, want 22", off)
	}
	if got := unsafe.Sizeof(entryPtr{}); got != HASH_ENTRY_SIZE {
		t.Fatalf("entryPtr size = %d, want %d", got, HASH_ENTRY_SIZE)
	}
}

func TestCodexLargeKeySetGetAndIterate(t *testing.T) {
	cache := NewCache(96 * 1024 * 1024)
	key := bytes.Repeat([]byte("tenant/device/timestamp/business/"), 2200)
	if len(key) <= 65535 {
		t.Fatalf("test key length %d is not large", len(key))
	}
	value := []byte("composite-key-value")

	if err := cache.Set(key, value, 0); err != nil {
		t.Fatalf("Set large key: %v", err)
	}
	if got, err := cache.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("Get large key: err=%v equal=%v", err, bytes.Equal(got, value))
	}
	if err := cache.GetFn(key, func(got []byte) error {
		if !bytes.Equal(got, value) {
			t.Fatal("GetFn value mismatch")
		}
		return nil
	}); err != nil {
		t.Fatalf("GetFn large key: %v", err)
	}
	sawIteratorEntry := false
	iterator := cache.NewIterator()
	for entry := iterator.Next(); entry != nil; entry = iterator.Next() {
		if bytes.Equal(entry.Key, key) && bytes.Equal(entry.Value, value) {
			sawIteratorEntry = true
		}
	}
	if !sawIteratorEntry {
		t.Fatal("iterator did not return the large key entry")
	}
	if !cache.Del(key) {
		t.Fatal("Del large key returned false")
	}
	if _, err := cache.Get(key); err != ErrNotFound {
		t.Fatalf("Get after Del: err=%v, want ErrNotFound", err)
	}
}

func TestCodexLargeKeyCapacityEvictionUsesExtendedLength(t *testing.T) {
	seg := newSegment(1024*1024, 0, &mockTimer{})
	key := bytes.Repeat([]byte("tenant/device/"), 5000)
	value := []byte("capacity-eviction-value")
	hashVal := hashFunc(key)
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)

	if err := seg.set(key, value, hashVal, 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	entryLen := entryPrefixSizeForKeyLen(len(key)) + int64(len(key)) + int64(len(value))
	if _, err := seg.rb.Write(make([]byte, 800000)); err != nil {
		t.Fatal(err)
	}
	seg.vacuumLen = seg.rb.Size() - seg.rb.End()
	seg.totalCount = 1
	seg.totalTime = 1 << 62
	beforeVacuum := seg.vacuumLen

	if !seg.evacuate(seg.vacuumLen+1, slotId, 1) {
		t.Fatal("evacuate did not report slot modification")
	}
	if seg.vacuumLen != beforeVacuum+entryLen {
		t.Fatalf("vacuumLen=%d, want %d", seg.vacuumLen, beforeVacuum+entryLen)
	}
	if _, match := seg.lookup(seg.getSlot(slotId), hash16, key); match {
		t.Fatal("extended entry survived capacity eviction")
	}
}

func TestCodexLargeKeySegmentMigration(t *testing.T) {
	seg := newSegment(1024*1024, 0, &mockTimer{})
	key := bytes.Repeat([]byte("k"), 70000)
	value := []byte("evacuate-value")
	hashVal := hashFunc(key)
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)

	if err := seg.set(key, value, hashVal, 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		t.Fatal("large key not found before evacuation")
	}
	oldOffset := slot[idx].offset
	entryLen := entryPrefixSizeForKeyLen(len(key)) + int64(len(key)) + int64(len(value))
	newOffset := seg.rb.Evacuate(oldOffset, int(entryLen))
	if newOffset < 0 {
		t.Fatal("ring buffer evacuation failed")
	}
	seg.updateEntryPtr(slotId, hash16, oldOffset, newOffset)

	slot = seg.getSlot(slotId)
	idx, match = seg.lookup(slot, hash16, key)
	if !match {
		t.Fatal("large key not found after evacuation")
	}
	if slot[idx].offset != newOffset || slot[idx].offset == oldOffset {
		t.Fatal("entry was not evacuated")
	}
	if got, _, err := seg.get(key, nil, hashVal, false); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("get after evacuation: err=%v equal=%v", err, bytes.Equal(got, value))
	}
}

func TestCodexLegacyKeyLayoutReadAndMigration(t *testing.T) {
	seg := newSegment(4096, 0, &mockTimer{})
	key := []byte("legacy-composite-key")
	value := []byte("legacy-value")
	hashVal := hashFunc(key)
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)

	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	hdr.accessTime = 100
	hdr.keyLen = uint16(len(key))
	hdr.hash16 = hash16
	hdr.valLen = uint32(len(value))
	hdr.valCap = uint32(len(value))
	hdr.slotId = slotId

	if _, err := seg.rb.Write(hdrBuf[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.rb.Write(key); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.rb.Write(value); err != nil {
		t.Fatal(err)
	}

	seg.slotLens[slotId] = 1
	slot := seg.getSlot(slotId)
	slot[0] = entryPtr{
		offset:   0,
		hash16:   hash16,
		keyLen:   uint16(len(key)),
		reserved: 0,
	}
	entryLen := int64(ENTRY_HDR_SIZE + len(key) + len(value))
	seg.vacuumLen = int64(seg.rb.Size()) - entryLen
	seg.totalCount = 1
	seg.totalTime = 99

	if got, _, err := seg.get(key, nil, hashVal, true); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("legacy get: err=%v equal=%v", err, bytes.Equal(got, value))
	}

	newOffset := seg.rb.Evacuate(0, int(entryLen))
	if newOffset < 0 {
		t.Fatal("legacy ring buffer evacuation failed")
	}
	seg.updateEntryPtr(slotId, hash16, 0, newOffset)
	slot = seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		t.Fatal("legacy key missing after evacuation")
	}
	if slot[idx].offset != newOffset || slot[idx].offset == 0 {
		t.Fatal("legacy entry was not copied by evacuation")
	}
	if got, _, err := seg.get(key, nil, hashVal, true); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("legacy get after evacuation: err=%v equal=%v", err, bytes.Equal(got, value))
	}
}

func TestCodexLegacyKeyLayoutCapacityEviction(t *testing.T) {
	seg := newSegment(4096, 0, &mockTimer{})
	key := []byte("legacy-eviction-key")
	value := []byte("legacy-eviction-value")
	hashVal := hashFunc(key)
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)

	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	hdr.accessTime = 100
	hdr.keyLen = uint16(len(key))
	hdr.hash16 = hash16
	hdr.valLen = uint32(len(value))
	hdr.valCap = uint32(len(value))
	hdr.slotId = slotId
	if _, err := seg.rb.Write(hdrBuf[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.rb.Write(key); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.rb.Write(value); err != nil {
		t.Fatal(err)
	}
	if _, err := seg.rb.Write(make([]byte, 3000)); err != nil {
		t.Fatal(err)
	}

	seg.slotLens[slotId] = 1
	slot := seg.getSlot(slotId)
	slot[0] = entryPtr{offset: 0, hash16: hash16, keyLen: uint16(len(key))}
	seg.vacuumLen = seg.rb.Size() - seg.rb.End()
	seg.totalCount = 1
	seg.totalTime = 100
	beforeVacuum := seg.vacuumLen

	if !seg.evacuate(seg.vacuumLen+1, slotId, 101) {
		t.Fatal("evacuate did not report slot modification")
	}
	wantVacuum := beforeVacuum + int64(ENTRY_HDR_SIZE+len(key)+len(value))
	if seg.vacuumLen != wantVacuum {
		t.Fatalf("vacuumLen=%d, want %d", seg.vacuumLen, wantVacuum)
	}
	if _, match := seg.lookup(seg.getSlot(slotId), hash16, key); match {
		t.Fatal("legacy entry survived capacity eviction")
	}
}
