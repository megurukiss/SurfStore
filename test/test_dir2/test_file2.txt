package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := &BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}
	for _, hash := range blockHashesIn.GetHashes() {
		blockAddr := m.ConsistentHashRing.GetResponsibleServer(hash)
		if _, ok := blockStoreMap.BlockStoreMap[blockAddr]; !ok {
			blockStoreMap.BlockStoreMap[blockAddr] = &BlockHashes{Hashes: []string{}}
		}
		blockStoreMap.BlockStoreMap[blockAddr].Hashes = append(blockStoreMap.BlockStoreMap[blockAddr].Hashes, hash)
	}
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	blockStoreAddrs := &BlockStoreAddrs{BlockStoreAddrs: make([]string, 0)}
	for _, addr := range m.BlockStoreAddrs {
		blockStoreAddrs.BlockStoreAddrs = append(blockStoreAddrs.BlockStoreAddrs, addr)
	}
	return blockStoreAddrs, nil
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	returnMap := make(map[string]*FileMetaData)
	for k, v := range m.FileMetaMap {
		returnMap[k] = &FileMetaData{
			Filename:      v.Filename,
			Version:       v.Version,
			BlockHashList: v.BlockHashList,
		}
	}
	return &FileInfoMap{FileInfoMap: returnMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.GetFilename()
	fileVersion := fileMetaData.GetVersion()
	fileVHashes := fileMetaData.GetBlockHashList()

	if serverMetaData, ok := m.FileMetaMap[fileName]; !ok {
		// create new metadata
		if fileVersion == 1 {
			m.FileMetaMap[fileName] = &FileMetaData{
				Filename:      fileName,
				Version:       fileVersion,
				BlockHashList: fileVHashes,
			}
			log.Printf("Created new file %s with version %d\n", fileName, fileVersion)
			return &Version{Version: fileVersion}, nil
		}
		// if version is not 1, return version -1
		log.Printf("Version mismatch for file %s, requested version: %d, expected version: 1\n", fileName, fileVersion)
		return &Version{Version: -1}, fmt.Errorf("Version mismatch for file %s, requested version: %d, expected version: 1", fileName, fileVersion)
	} else {
		// update existing metadata
		// check version
		currentVersion := serverMetaData.GetVersion()
		if currentVersion+1 == fileVersion {
			// update the file
			serverMetaData.Version = fileVersion
			serverMetaData.BlockHashList = fileVHashes

			log.Printf("Updated file %s to version %d\n", fileName, fileVersion)
			return &Version{Version: fileVersion}, nil
		} else {
			// version mismatch
			log.Printf("Version mismatch for file %s, current version: %d, received version: %d\n", fileName, currentVersion, fileVersion)
			// return version -1
			return &Version{Version: -1}, fmt.Errorf("Version mismatch for file %s, current version: %d, received version: %d", fileName, currentVersion, fileVersion)
		}
	}
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
