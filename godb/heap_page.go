package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

/*
HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.
*/
type Header struct {
	TotalSlots int32
	UsedSlots  int32
}
type RecordID struct {
	PageNo int
	SlotNo int
}
type heapPage struct {
	// TODO: some code goes here
	Hdr   Header
	Tuple []*Tuple
	Desc  *TupleDesc

	File   *HeapFile
	PageNo int
	Dirty  bool

	DataLength int //useful date from initFromBuffer
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	bytesPerTuple := 0
	for _, field := range desc.Fields {
		if field.Ftype == StringType {
			bytesPerTuple += int(unsafe.Sizeof(byte('a'))) * StringLength
		} else if field.Ftype == IntType {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
	}
	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple

	return &heapPage{
		Hdr:    Header{TotalSlots: int32(numSlots), UsedSlots: 0},
		Tuple:  make([]*Tuple, numSlots),
		Desc:   desc,
		File:   f,
		PageNo: pageNo,
	}
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	free := h.Hdr.TotalSlots - h.Hdr.UsedSlots
	return int(free) //replace me
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	rid := RecordID{PageNo: h.PageNo, SlotNo: 0}
	if h.Hdr.UsedSlots == h.Hdr.TotalSlots {
		return rid, errors.New("no free slots")
	}
	for i, tuple := range h.Tuple {
		if tuple == nil {
			h.Tuple[i] = t
			rid.SlotNo = i
			h.Tuple[i].Rid = rid
			h.Hdr.UsedSlots += 1
			return rid, nil
		}
	}
	return rid, errors.New("can't insert Tuple") //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	for i, tuple := range h.Tuple {
		if tuple != nil && tuple.Rid.(RecordID).SlotNo == rid.(RecordID).SlotNo {
			h.Tuple[i] = nil
			h.Hdr.UsedSlots -= 1
			return nil
		}
	}
	return errors.New("the slot is invalid") //replace me

}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.Dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
	h.Dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	// TODO: some code goes here
	var dbFile DBFile = p.File
	return &dbFile //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	bytesBuffer := new(bytes.Buffer)
	err := binary.Write(bytesBuffer, binary.LittleEndian, h.Hdr)
	if err != nil {
		return nil, err
	}
	for _, tuple := range h.Tuple {
		if tuple != nil {
			err := tuple.writeTo(bytesBuffer)
			if err != nil {
				return nil, err
			}
		}
	}
	paddingSize := 4096 - bytesBuffer.Len()
	paddingBytes := make([]byte, paddingSize)
	_, err = bytesBuffer.Write(paddingBytes)
	if err != nil {
		return nil, err
	}

	return bytesBuffer, nil //replace me
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	err := binary.Read(buf, binary.LittleEndian, &h.Hdr)
	if err != nil {
		return err
	}
	for i := 0; i < int(h.Hdr.UsedSlots); i++ {
		tuple, err := readTupleFrom(buf, h.Desc)
		if err != nil {
			return err
		}
		h.Tuple[i] = tuple
		rid := RecordID{PageNo: h.PageNo, SlotNo: i}
		h.Tuple[i].Rid = rid
	}
	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	i := 0
	return func() (*Tuple, error) {
		for i < int(p.Hdr.TotalSlots) && p.Tuple[i] == nil {
			i++
		}
		if i <= int(p.Hdr.TotalSlots) && i < len(p.Tuple) {
			if p.Tuple[i] != nil { //p.Tuple[i] won't be nil in usual.So It just a double check.
				tuple := p.Tuple[i]
				tuple.Rid = p.Tuple[i].Rid
				i++
				return tuple, nil
			}
		}
		return nil, nil
	}
}
