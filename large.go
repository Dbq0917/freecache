package freecache

import (
	"encoding/binary"
	"errors"
	"math"
	"sync/atomic"
	"unsafe"
)

const (
	largeEntryMagic uint16 = 0x4C47
	largeChunkMagic uint16 = 0x434B

	largeDescMagic     uint32 = 0x3146434C
	largeDescHdrSize          = 8
	largeOffsetSize           = 8
	largeChunkMetaSize        = 8
	largeChunkSlotID   uint8  = 255
)

var errLargeLockBusy = errors.New("large entry chunk lock is busy")

func (seg *segment) maxNormalEntryLen() int {
	return len(seg.rb.data)/4 - ENTRY_HDR_SIZE
}

func (cache *Cache) lockAllSegments() {
	for segID := 0; segID < segmentCount; segID++ {
		cache.locks[segID].Lock()
	}
}

func (cache *Cache) unlockAllSegments() {
	for segID := segmentCount - 1; segID >= 0; segID-- {
		cache.locks[segID].Unlock()
	}
}

func (cache *Cache) setLargeEntry(segID int, key, value []byte, hashVal uint64, expireSeconds int) error {
	cache.lockAllSegments()
	defer cache.unlockAllSegments()
	return cache.segments[segID].setLargeLocked(key, value, hashVal, expireSeconds)
}

func (cache *Cache) getLargeSlow(segID int, key, buf []byte, hashVal uint64, peek bool) ([]byte, uint32, error) {
	cache.locks[segID].Unlock()
	cache.lockAllSegments()
	value, expireAt, err := cache.segments[segID].getAllLocked(key, buf, hashVal, peek)
	cache.unlockAllSegments()
	cache.locks[segID].Lock()
	return value, expireAt, err
}

func largeChunkSegment(homeSegID, chunkIndex int) int {
	return (homeSegID + 1 + chunkIndex) & segmentAndOpVal
}

func encodeLargeDescriptor(chunkSize uint32, offsets []int64) []byte {
	desc := make([]byte, largeDescHdrSize+largeOffsetSize*len(offsets))
	binary.LittleEndian.PutUint32(desc[0:4], largeDescMagic)
	binary.LittleEndian.PutUint32(desc[4:8], chunkSize)
	for i, offset := range offsets {
		binary.LittleEndian.PutUint64(desc[largeDescHdrSize+i*largeOffsetSize:], uint64(offset))
	}
	return desc
}

func parseLargeDescriptor(desc []byte) (chunkSize uint32, offsets []int64, err error) {
	if len(desc) < largeDescHdrSize {
		return 0, nil, ErrNotFound
	}
	if binary.LittleEndian.Uint32(desc[0:4]) != largeDescMagic {
		return 0, nil, ErrNotFound
	}
	chunkSize = binary.LittleEndian.Uint32(desc[4:8])
	offsetBytes := len(desc) - largeDescHdrSize
	if offsetBytes%largeOffsetSize != 0 {
		return 0, nil, ErrNotFound
	}
	count := offsetBytes / largeOffsetSize
	offsets = make([]int64, count)
	for i := 0; i < count; i++ {
		offsets[i] = int64(binary.LittleEndian.Uint64(desc[largeDescHdrSize+i*largeOffsetSize:]))
	}
	return chunkSize, offsets, nil
}

func (seg *segment) setLargeLocked(key, value []byte, hashVal uint64, expireSeconds int) error {
	if len(key) > 65535 || len(value) > math.MaxUint32 {
		return ErrLargeKey
	}
	ringSize := int64(len(seg.rb.data))
	chunkBlockLen := ringSize / 4
	chunkPayload := int(chunkBlockLen) - ENTRY_HDR_SIZE - largeChunkMetaSize
	if chunkPayload <= 0 {
		return ErrLargeEntry
	}

	chunkCount := 0
	if len(value) > 0 {
		chunkCount = (len(value) + chunkPayload - 1) / chunkPayload
	}
	descLen := largeDescHdrSize + largeOffsetSize*chunkCount
	parentEntryLen := int64(ENTRY_HDR_SIZE + len(key) + descLen)
	if parentEntryLen > ringSize {
		return ErrLargeEntry
	}

	var chunkCounts [segmentCount]int
	for chunkIndex := 0; chunkIndex < chunkCount; chunkIndex++ {
		chunkCounts[largeChunkSegment(seg.segId, chunkIndex)]++
	}
	for targetSegID := 0; targetSegID < segmentCount; targetSegID++ {
		needed := int64(chunkCounts[targetSegID]) * chunkBlockLen
		if targetSegID == seg.segId {
			needed += parentEntryLen
		}
		if needed > ringSize {
			return ErrLargeEntry
		}
	}

	now := seg.timer.Now()
	expireAt := expireAtFrom(now, expireSeconds)
	slotID := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotID)
	idx, match := seg.lookup(slot, hash16, key)
	if match {
		seg.delEntryPtr(slotID, slot, idx)
	}

	offsets := make([]int64, 0, chunkCount)
	for start, chunkIndex := 0, 0; start < len(value); start, chunkIndex = start+chunkPayload, chunkIndex+1 {
		end := start + chunkPayload
		if end > len(value) {
			end = len(value)
		}
		chunk := value[start:end]
		targetSegID := largeChunkSegment(seg.segId, chunkIndex)
		targetSeg := &seg.cache.segments[targetSegID]
		targetSeg.evacuate(chunkBlockLen, largeChunkSlotID, now)

		offset := targetSeg.rb.End()
		var chunkHdrBuf [ENTRY_HDR_SIZE]byte
		chunkHdr := (*entryHdr)(unsafe.Pointer(&chunkHdrBuf[0]))
		chunkHdr.accessTime = now
		chunkHdr.expireAt = expireAt
		chunkHdr.keyLen = largeChunkMetaSize
		chunkHdr.valLen = uint32(len(chunk))
		chunkHdr.valCap = uint32(chunkPayload)
		chunkHdr.reserved = largeChunkMagic
		if _, err := targetSeg.rb.Write(chunkHdrBuf[:]); err != nil {
			return ErrLargeEntry
		}
		var checksumBuf [largeChunkMetaSize]byte
		binary.LittleEndian.PutUint64(checksumBuf[:], hashFunc(chunk))
		if _, err := targetSeg.rb.Write(checksumBuf[:]); err != nil {
			return ErrLargeEntry
		}
		if _, err := targetSeg.rb.Write(chunk); err != nil {
			return ErrLargeEntry
		}
		if padding := chunkPayload - len(chunk); padding > 0 {
			targetSeg.rb.Skip(int64(padding))
		}
		targetSeg.vacuumLen -= chunkBlockLen
		offsets = append(offsets, offset)
	}

	desc := encodeLargeDescriptor(uint32(chunkPayload), offsets)
	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	hdr.slotId = slotID
	hdr.hash16 = hash16
	hdr.keyLen = uint16(len(key))
	hdr.accessTime = now
	hdr.expireAt = expireAt
	hdr.valLen = uint32(len(value))
	hdr.valCap = uint32(len(desc))
	hdr.reserved = largeEntryMagic

	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(len(desc))
	slotModified := seg.evacuate(entryLen, slotID, now)
	if slotModified {
		slot = seg.getSlot(slotID)
		idx, match = seg.lookup(slot, hash16, key)
	}
	newOffset := seg.rb.End()
	seg.insertEntryPtr(slotID, hash16, newOffset, idx, hdr.keyLen)
	seg.rb.Write(hdrBuf[:])
	seg.rb.Write(key)
	seg.rb.Write(desc)
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	seg.vacuumLen -= entryLen
	return nil
}

func (seg *segment) readLargeValue(hdr *entryHdr, ptrOffset int64, buf []byte, allLocksHeld bool) (value []byte, err error) {
	descOffset := ptrOffset + ENTRY_HDR_SIZE + int64(hdr.keyLen)
	desc := make([]byte, hdr.valCap)
	if _, err = seg.rb.ReadAt(desc, descOffset); err != nil {
		return nil, ErrNotFound
	}
	chunkSize, offsets, err := parseLargeDescriptor(desc)
	if err != nil || len(offsets) == 0 && hdr.valLen != 0 {
		return nil, ErrNotFound
	}
	totalLen := hdr.valLen
	if totalLen > 0 {
		wantCount := (uint64(totalLen) + uint64(chunkSize) - 1) / uint64(chunkSize)
		if chunkSize == 0 || uint64(len(offsets)) != wantCount {
			return nil, ErrNotFound
		}
	}
	if cap(buf) >= int(totalLen) {
		value = buf[:totalLen]
	} else {
		value = make([]byte, totalLen)
	}

	cache := seg.cache
	var locked [segmentCount]bool
	if !allLocksHeld {
		for chunkIndex := range offsets {
			targetSegID := largeChunkSegment(seg.segId, chunkIndex)
			if targetSegID == seg.segId || locked[targetSegID] {
				continue
			}
			if cache == nil || !cache.locks[targetSegID].TryLock() {
				for lockedID, wasLocked := range locked {
					if wasLocked {
						cache.locks[lockedID].Unlock()
					}
				}
				return nil, errLargeLockBusy
			}
			locked[targetSegID] = true
		}
		defer func() {
			for lockedID, wasLocked := range locked {
				if wasLocked {
					cache.locks[lockedID].Unlock()
				}
			}
		}()
	}

	var written uint32
	for chunkIndex, offset := range offsets {
		chunkLen := chunkSize
		if chunkIndex == len(offsets)-1 {
			chunkLen = totalLen - written
		}
		if chunkLen > chunkSize || uint64(written)+uint64(chunkLen) > uint64(totalLen) {
			return nil, ErrNotFound
		}
		targetSegID := largeChunkSegment(seg.segId, chunkIndex)
		targetSeg := seg
		if targetSegID != seg.segId {
			if cache == nil {
				return nil, ErrNotFound
			}
			targetSeg = &cache.segments[targetSegID]
		}
		var chunkHdrBuf [ENTRY_HDR_SIZE]byte
		if _, err = targetSeg.rb.ReadAt(chunkHdrBuf[:], offset); err != nil {
			return nil, ErrNotFound
		}
		chunkHdr := (*entryHdr)(unsafe.Pointer(&chunkHdrBuf[0]))
		chunkEnd := int(written) + int(chunkLen)
		if chunkEnd > len(value) {
			return nil, ErrNotFound
		}
		chunk := value[written:chunkEnd]
		var checksumBuf [largeChunkMetaSize]byte
		if chunkHdr.keyLen != largeChunkMetaSize {
			return nil, ErrNotFound
		}
		dataOffset := offset + ENTRY_HDR_SIZE + int64(chunkHdr.keyLen)
		if _, err = targetSeg.rb.ReadAt(checksumBuf[:], offset+ENTRY_HDR_SIZE); err != nil {
			return nil, ErrNotFound
		}
		if _, err = targetSeg.rb.ReadAt(chunk, dataOffset); err != nil {
			return nil, ErrNotFound
		}
		if chunkHdr.reserved != largeChunkMagic || chunkHdr.valLen != chunkLen ||
			binary.LittleEndian.Uint64(checksumBuf[:]) != hashFunc(chunk) {
			return nil, ErrNotFound
		}
		written += chunkLen
	}
	if written != totalLen {
		return nil, ErrNotFound
	}
	return value, nil
}
