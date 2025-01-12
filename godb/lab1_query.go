package godb

import (
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.

func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	sum := 0
	lab1_bp := "lab1bp.dat"
	if _, err := os.Stat(lab1_bp); err == nil {
		os.Remove(lab1_bp)
	} else {
		f, err := os.OpenFile(lab1_bp, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return 0, err
		}
		defer f.Close()
	}
	bp := NewBufferPool(30)
	hp, err := NewHeapFile(lab1_bp, &td, bp)
	if err != nil {
		return 0, err
	}
	csvFile, _ := os.Open(fileName)
	err = hp.LoadFromCSV(csvFile, true, ",", false)
	if err != nil {
		return 0, nil
	}
	tid := NewTID()
	iter, _ := hp.Iterator(tid)
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		for _, field := range t.Fields {
			if _, a := field.(IntField); a {
				sum = sum + (int)(field.(IntField).Value)
			}
		}
	}
	return sum, nil // replace me
}
