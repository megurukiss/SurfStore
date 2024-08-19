package test

import (
	"cse224/proj4/pkg/surfstore"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestWriteFileMeta(t *testing.T) {
	baseDir := "/Users/meguru_kiss/workspace/assignments/surfstore/proj3/test"
	fileMetas := map[string]*surfstore.FileMetaData{
		"file1": {
			Filename:      "file1",
			Version:       1,
			BlockHashList: []string{"hash1", "hash2"},
		},
	}
	// test WriteMetaFile
	err := surfstore.WriteMetaFile(fileMetas, baseDir)
	if err != nil {
		t.Errorf("WriteMetaFile failed: %v", err)
	}
}

func TestReadFileMeta(t *testing.T) {
	baseDir := "/Users/meguru_kiss/workspace/assignments/surfstore/proj3/test"
	// test ReadMetaFile
	fileMetas, err := surfstore.LoadMetaFromMetaFile(baseDir)
	if err != nil {
		t.Errorf("ReadMetaFile failed: %v", err)
	}
	fmt.Println(fileMetas)
	fmt.Println(len(fileMetas["file1"].BlockHashList))
}
