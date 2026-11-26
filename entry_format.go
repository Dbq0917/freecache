package freecache

import (
	"encoding/binary"
	"math"
)

const entryExtKeyLenSize = 4

const entryHdrExtKeyLenMagic uint16 = 0x4C4B

const (
	// MaxKeyLength is the largest representable key length.
	MaxKeyLength = math.MaxUint32
	// MaxKeyLen is an alias for MaxKeyLength.
	MaxKeyLen = MaxKeyLength
	// MaxKeySize is an alias for MaxKeyLength.
	MaxKeySize = MaxKeyLength
)

func keyLengthSupported(keyLen int) bool {
	return uint64(keyLen) <= MaxKeyLength
}

func entryExtSizeForKeyLen(keyLen int) int64 {
	if keyLen > math.MaxUint16 {
		return entryExtKeyLenSize
	}
	return 0
}

func entryPrefixSizeForKeyLen(keyLen int) int64 {
	return ENTRY_HDR_SIZE + entryExtSizeForKeyLen(keyLen)
}

func entryKeyOffset(offset int64, keyLen int) int64 {
	return offset + entryPrefixSizeForKeyLen(keyLen)
}

func entryValueOffset(offset int64, keyLen int) int64 {
	return entryKeyOffset(offset, keyLen) + int64(keyLen)
}

func hdrHasExtKeyLen(hdr *entryHdr) bool {
	return hdr.reserved == entryHdrExtKeyLenMagic && hdr.keyLen == math.MaxUint16
}

func hdrPrefixSize(hdr *entryHdr) int64 {
	if hdrHasExtKeyLen(hdr) {
		return ENTRY_HDR_SIZE + entryExtKeyLenSize
	}
	return ENTRY_HDR_SIZE
}

func hdrKeyLength(seg *segment, offset int64, hdr *entryHdr) uint32 {
	if !hdrHasExtKeyLen(hdr) {
		return uint32(hdr.keyLen)
	}
	var extLen [entryExtKeyLenSize]byte
	_, _ = seg.rb.ReadAt(extLen[:], offset+ENTRY_HDR_SIZE)
	return binary.NativeEndian.Uint32(extLen[:])
}

func encodeHdrKeyLen(hdr *entryHdr, keyLen int) (extLen [entryExtKeyLenSize]byte, hasExt bool) {
	if keyLen > math.MaxUint16 {
		hdr.keyLen = math.MaxUint16
		hdr.reserved = entryHdrExtKeyLenMagic
		binary.NativeEndian.PutUint32(extLen[:], uint32(keyLen))
		return extLen, true
	}
	hdr.keyLen = uint16(keyLen)
	hdr.reserved = 0
	return extLen, false
}

func (ptr *entryPtr) setKeyLength(keyLen uint32) {
	ptr.keyLen = uint16(keyLen)
	ptr.reserved = keyLen
}

func (ptr *entryPtr) keyLength() uint32 {
	if ptr.reserved != 0 {
		return ptr.reserved
	}
	return uint32(ptr.keyLen)
}
