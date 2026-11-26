package freecache

import (
	"container/heap"
	"math"
	"sync/atomic"
	"unsafe"
)

type freeRegion struct {
	off     int64
	length  int64
	entries int64
	timeSum int64
}

type freeBlock struct {
	off    int64
	length int64
}

type freeRegionHeap []freeBlock

func (h freeRegionHeap) Len() int { return len(h) }

func (h freeRegionHeap) Less(i, j int) bool {
	if h[i].length != h[j].length {
		return h[i].length > h[j].length
	}
	return h[i].off < h[j].off
}

func (h freeRegionHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *freeRegionHeap) Push(x interface{}) {
	*h = append(*h, x.(freeBlock))
}

func (h *freeRegionHeap) Pop() interface{} {
	old := *h
	n := len(old)
	block := old[n-1]
	*h = old[:n-1]
	return block
}

func (seg *segment) addFreeRegion(off, length, entries, timeSum int64) {
	if length < ENTRY_HDR_SIZE || entries <= 0 || off < 0 {
		return
	}
	if seg.freeRegions == nil {
		seg.freeRegions = make(map[int64]freeRegion)
		seg.freeEnds = make(map[int64]int64)
	}

	start := off
	regionEnd := off + length
	if prevStart, ok := seg.freeEnds[off]; ok {
		prev := seg.freeRegions[prevStart]
		start = prev.off
		length += prev.length
		entries += prev.entries
		timeSum += prev.timeSum
		delete(seg.freeRegions, prevStart)
		delete(seg.freeEnds, off)
	}
	if next, ok := seg.freeRegions[regionEnd]; ok {
		length += next.length
		entries += next.entries
		timeSum += next.timeSum
		delete(seg.freeRegions, regionEnd)
		delete(seg.freeEnds, regionEnd+next.length)
	}

	region := freeRegion{off: start, length: length, entries: entries, timeSum: timeSum}
	seg.freeRegions[start] = region
	seg.freeEnds[start+length] = start
	heap.Push(&seg.freeHeap, freeBlock{off: start, length: length})

	if len(seg.freeHeap) > 2*len(seg.freeRegions)+32 {
		seg.compactFreeHeap()
	}
}

func (seg *segment) compactFreeHeap() {
	compact := make(freeRegionHeap, 0, len(seg.freeRegions))
	for _, region := range seg.freeRegions {
		compact = append(compact, freeBlock{off: region.off, length: region.length})
	}
	heap.Init(&compact)
	seg.freeHeap = compact
}

func (seg *segment) detachFreeRegion(off int64) {
	region, ok := seg.freeRegions[off]
	if !ok {
		return
	}
	delete(seg.freeRegions, off)
	delete(seg.freeEnds, off+region.length)
}

// recycleDeletedRegion immediately advances the vacuum frontier when the
// deleted interval is the next interval evacuation would visit. Otherwise the
// interval is retained for direct hole allocation.
func (seg *segment) recycleDeletedRegion(off, length int64, accessTime uint32) {
	frontier := seg.rb.End() + seg.vacuumLen - seg.rb.Size()
	if off == frontier {
		seg.vacuumLen += length
		atomic.AddInt64(&seg.totalTime, -int64(accessTime))
		atomic.AddInt64(&seg.totalCount, -1)
		return
	}
	seg.addFreeRegion(off, length, 1, int64(accessTime))
}

// allocFreeRegion reuses a deleted interval. The planned prefix keeps the
// established valCap growth policy. A tail large enough for a header becomes a
// synthetic deleted region; a smaller tail is absorbed into the new capacity.
func (seg *segment) allocFreeRegion(minLength int64, keyLen int, now uint32) (off, entryLen int64, reused bool) {
	for len(seg.freeHeap) > 0 {
		top := seg.freeHeap[0]
		region, ok := seg.freeRegions[top.off]
		if !ok || region.length != top.length {
			heap.Pop(&seg.freeHeap)
			continue
		}
		if region.length < minLength {
			return 0, 0, false
		}

		entryLen = minLength
		remainingLen := region.length - entryLen
		if 0 < remainingLen && remainingLen < ENTRY_HDR_SIZE {
			if region.length-ENTRY_HDR_SIZE-int64(keyLen) > math.MaxUint32 {
				return 0, 0, false
			}
			entryLen = region.length
			remainingLen = 0
		}

		heap.Pop(&seg.freeHeap)
		delete(seg.freeRegions, region.off)
		delete(seg.freeEnds, region.off+region.length)

		atomic.AddInt64(&seg.totalCount, -region.entries)
		atomic.AddInt64(&seg.totalTime, -region.timeSum)

		if remainingLen > 0 {
			remOff := region.off + entryLen
			tombstones, tombstoneTime := seg.writeDeletedTombstoneChain(remOff, remainingLen, now)
			seg.addFreeRegion(
				remOff,
				remainingLen,
				tombstones,
				tombstoneTime,
			)
			atomic.AddInt64(&seg.totalCount, tombstones)
			atomic.AddInt64(&seg.totalTime, tombstoneTime)
		}
		return region.off, entryLen, true
	}
	return 0, 0, false
}

func (seg *segment) writeDeletedTombstoneChain(off, length int64, now uint32) (count, timeSum int64) {
	if length < ENTRY_HDR_SIZE {
		return 0, 0
	}
	maxRecordLen := int64(math.MaxUint32) + ENTRY_HDR_SIZE
	count = (length-1)/maxRecordLen + 1
	capacity := length - count*ENTRY_HDR_SIZE
	timeSum = count * int64(now)
	pos := off

	for i := int64(0); i < count; i++ {
		recordCap := capacity
		if recordCap > math.MaxUint32 {
			recordCap = math.MaxUint32
		}
		var hdrBuf [ENTRY_HDR_SIZE]byte
		hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
		hdr.accessTime = now
		hdr.valCap = uint32(recordCap)
		hdr.deleted = true
		seg.rb.WriteAt(hdrBuf[:], pos)

		capacity -= recordCap
		pos += ENTRY_HDR_SIZE + recordCap
	}
	return count, timeSum
}
