package main

import "fmt"
import "os"
import "testing"

var kTestFilePath = "/tmp/test.txt"

func AssertOffset(t *testing.T, err error, offset, expect int64) {
	if err != nil {
		t.Fatal("Write fail:", err)
	}
	if offset != expect {
		t.Fatal("Write offset expect: ", expect, " but: ", offset)
	}
}

func TestWrite(t *testing.T) {
	store, err := NewDumbStore(kTestFilePath)
	if err != nil {
		t.Fatal("Initialize store failed.")
	}
	var offset int64
	offset, err = store.Write([]byte("hello"))
	AssertOffset(t, err, offset, 0)
	offset, err = store.Write([]byte("world"))
	AssertOffset(t, err, offset, 5)
	offset, err = store.Write([]byte(""))
	AssertOffset(t, err, offset, 10)
	offset, err = store.Write([]byte("!"))
	AssertOffset(t, err, offset, 10)
}

func TestMain(m *testing.M) {
	if err := os.Remove(kTestFilePath); err != nil {
		if err != os.ErrNotExist {
			fmt.Println("Setup testing err:", err)
			return
		}
	}
	os.Exit(m.Run())
}