package surfstore

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
)

func CompareHashList(hashList1 []string, hashList2 []string) bool {
	if len(hashList1) != len(hashList2) {
		return false
	}
	for i := range hashList1 {
		if hashList1[i] != hashList2[i] {
			return false
		}
	}
	return true
}

func RelativePath(baseDir string, path string) string {
	relativePath, err := filepath.Rel(baseDir, path)
	if err != nil {
		return ""
	}
	return relativePath
}

// retrieve hash list from a file
func GetFileHashList(blockSize int32, filePath string) []string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error During File Open")
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	numBlocks := int(fileSize) / int(blockSize)
	if int(fileSize)%int(blockSize) != 0 {
		numBlocks++
	}
	hashList := make([]string, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blockData := make([]byte, blockSize)
		n, err := file.Read(blockData)
		if err != nil {
			log.Fatal("Error During File Read")
		}
		hashList[i] = GetBlockHashString(blockData[:n])
	}
	return hashList
}

// retrieve block list from a file
func GetFileBlockList(blockSize int32, filePath string) []*Block {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error During File Open")
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	numBlocks := int(fileSize) / int(blockSize)
	if int(fileSize)%int(blockSize) != 0 {
		numBlocks++
	}
	blockList := make([]*Block, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blockData := make([]byte, blockSize)
		n, err := file.Read(blockData)
		if err != nil {
			log.Fatal("Error During File Read")
		}
		blockList[i] = &Block{BlockData: blockData[:n], BlockSize: int32(n)}
	}
	return blockList
}

func HashListMapFromFolder(baseDir string, blockSize int32) map[string][]string {
	hashListMap := make(map[string][]string)
	filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			if info.Name() == DEFAULT_META_FILENAME {
				return nil
			}
			relPath := RelativePath(baseDir, path)
			hashListMap[relPath] = GetFileHashList(blockSize, path)
		}
		return nil
	})
	return hashListMap
}

var localMap map[string][]string
var indexMetaMap map[string]*FileMetaData
var remoteMetaMap map[string]*FileMetaData
var localNewFile []string
var localDeleteFile []string
var localModifyFile []string
var downloadFiles []string
var remoteModifiedFiles []string

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var err error
	log.Printf("Client Base Dir: %v\n", client.BaseDir)
	log.Printf("Client MetaStore Addr: %v\n", client.MetaStoreAddr)
	log.Printf("Client BlockSize: %v\n", client.BlockSize)

	// check if the index file exists
	indexFilePath := filepath.Join(client.BaseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		log.Printf("Index file does not exist, creating a new one\n")
		WriteMetaFile(nil, client.BaseDir)
	}
	// get local file info map
	localMap = HashListMapFromFolder(client.BaseDir, int32(client.BlockSize))
	// load index file
	indexMetaMap, err = LoadMetaFromMetaFile(client.BaseDir)
	check_err(err)
	// get server file info map
	remoteMetaMap = make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteMetaMap)
	check_err(err)

	localNewFile = make([]string, 0)
	localDeleteFile = make([]string, 0)
	localModifyFile = make([]string, 0)
	downloadFiles = make([]string, 0)
	remoteModifiedFiles = make([]string, 0)

	for filename, remoteMeta := range remoteMetaMap {
		indexMeta, okIndex := indexMetaMap[filename]
		localHashes, okLocal := localMap[filename]

		// if not in local and not in index, download
		if !okLocal && !okIndex {
			downloadFiles = append(downloadFiles, filename)
			continue
		}

		// if not in index, but in local, check if need modify
		if !okIndex && okLocal {
			// check hash list
			if !CompareHashList(localHashes, remoteMeta.BlockHashList) {
				downloadFiles = append(downloadFiles, filename)
			}
			continue
		}

		// if in index, but not in local, check if need delete
		if okIndex && !okLocal {
			if remoteMeta.Version > indexMeta.Version {
				if !CompareHashList(remoteMeta.BlockHashList, []string{"0"}) {
					downloadFiles = append(downloadFiles, filename)
				}
			} else if remoteMeta.Version == indexMeta.Version {
				if !CompareHashList(remoteMeta.BlockHashList, []string{"0"}) {
					localDeleteFile = append(localDeleteFile, filename)
				}
			} else {
				log.Fatalf("Remote version is smaller than local version\n")
			}
			continue
		}

		// if in index and in local, check if need modify
		if okIndex && okLocal {
			if indexMeta.Version < remoteMeta.Version {
				remoteModifiedFiles = append(remoteModifiedFiles, filename)
			} else if indexMeta.Version == remoteMeta.Version {
				if !CompareHashList(localHashes, remoteMeta.BlockHashList) {
					localModifyFile = append(localModifyFile, filename)
				}
			}
		}
	}

	// check local files
	for fileName := range localMap {
		_, ok := remoteMetaMap[fileName]
		if !ok {
			localNewFile = append(localNewFile, fileName)
		}
	}

	// update local changes
	updateLocalToRemote(client)
	// update meta for modified files
	err = client.GetFileInfoMap(&remoteMetaMap)
	check_err(err)
	// update remote changes
	updateRemoteToLocal(client)
	// write meta data to local index
	WriteMetaFile(remoteMetaMap, client.BaseDir)
}

func updateLocalToRemote(client RPCClient) {
	for _, filename := range localNewFile {
		filePath := filepath.Join(client.BaseDir, filename)
		f, err := os.Open(filePath)
		check_err(err)
		fileBlockList := GetFileBlockList(int32(client.BlockSize), filePath)

		// build hash map
		fileBlockHashMap := make(map[string]*Block, len(fileBlockList))
		for _, block := range fileBlockList {
			hashString := GetBlockHashString(block.BlockData)
			fileBlockHashMap[hashString] = block
		}
		// get block store map
		blockStoreMap := make(map[string][]string)
		fileBlockHashList := make([]string, 0)
		for hash := range fileBlockHashMap {
			fileBlockHashList = append(fileBlockHashList, hash)
		}
		err = client.GetBlockStoreMap(fileBlockHashList, &blockStoreMap)
		check_err(err)

		// put blocks
		for addr, blockHashList := range blockStoreMap {
			for _, hash := range blockHashList {
				succ := true
				block := fileBlockHashMap[hash]
				err = client.PutBlock(block, addr, &succ)
				check_err(err)
				if !succ {
					log.Fatalf("Put block failed\n")
				}
			}
		}

		f.Close()
		// update meta data
		fileHashes := localMap[filename]
		var latestVersion int32
		err = client.UpdateFile(&FileMetaData{Filename: filename, Version: 1, BlockHashList: fileHashes}, &latestVersion)
		check_err(err)
		if latestVersion == -1 {
			// version mismatch
			remoteModifiedFiles = append(remoteModifiedFiles, filename)
		} else if latestVersion != 1 {
			log.Fatalf("Version mismatch\n")
		}
	}

	// delete files
	for _, filename := range localDeleteFile {
		localVersion := indexMetaMap[filename].Version
		var latestVersion int32
		err := client.UpdateFile(&FileMetaData{Filename: filename, Version: localVersion + 1, BlockHashList: []string{"0"}}, &latestVersion)
		check_err(err)
		if latestVersion == -1 {
			// version mismatch
			downloadFiles = append(downloadFiles, filename)
		} else if latestVersion != localVersion+1 {
			log.Fatalf("Version mismatch\n")
		}
	}

	// modify files
	for _, filename := range localModifyFile {
		filepath := filepath.Join(client.BaseDir, filename)
		f, err := os.Open(filepath)
		check_err(err)
		// get the blocks not in remote
		localHashes := localMap[filename]

		// get all addresses
		blockstoreAddresses := make([]string, 0)
		err = client.GetBlockStoreAddrs(&blockstoreAddresses)
		check_err(err)

		// get missing hashes for each block store
		var missingHashes []string
		for _, blockAddress := range blockstoreAddresses {
			tempHashes := make([]string, 0)
			err = client.MissingBlocks(localHashes, blockAddress, &tempHashes)
			check_err(err)
			missingHashes = append(missingHashes, tempHashes...)
		}

		// sort missing hashes based on local hashes order
		indexMap := make(map[string]int)
		for i, hash := range localHashes {
			indexMap[hash] = i
		}
		sort.Slice(missingHashes, func(i, j int) bool {
			return indexMap[missingHashes[i]] < indexMap[missingHashes[j]]
		})

		// get block store address map for missing hashes
		blockStoreMap := make(map[string][]string)
		err = client.GetBlockStoreMap(missingHashes, &blockStoreMap)
		check_err(err)
		// build hash address map
		hashAddressMap := make(map[string]string)
		for addr, hashList := range blockStoreMap {
			for _, hash := range hashList {
				hashAddressMap[hash] = addr
			}
		}

		// iterate through hashes
		hashIndex := 0
		seekSize := int64(client.BlockSize)
		block := make([]byte, client.BlockSize)
		var success bool
		for _, hash := range localHashes {
			if hash == missingHashes[hashIndex] {
				// read the block
				n, err := f.Read(block)
				if err != nil && err != io.EOF {
					log.Fatalf("Error when reading file\n")
				}

				// put the block
				blockAddress := hashAddressMap[hash]
				client.PutBlock(&Block{BlockData: block[:n], BlockSize: int32(n)}, blockAddress, &success)
				if !success {
					log.Fatalf("Put Block Failed\n")
				}
				// move to the next hash
				hashIndex++
				// check if the hashIndex is out of bound
				if hashIndex == len(missingHashes) {
					break
				}
			} else {
				// skip the block
				_, err = f.Seek(seekSize, 1)
				check_err(err)
			}
		}
		// update meta data
		localVersion := indexMetaMap[filename].Version
		var latestVersion int32
		err = client.UpdateFile(&FileMetaData{Filename: filename, Version: localVersion + 1, BlockHashList: localHashes}, &latestVersion)
		check_err(err)
		if latestVersion == -1 {
			// version mismatch
			remoteModifiedFiles = append(remoteModifiedFiles, filename)
		} else if latestVersion != localVersion+1 {
			log.Fatalf("Version mismatch\n")
		}

		f.Close()
	}
}

func updateRemoteToLocal(client RPCClient) {
	// download files
	for _, filename := range downloadFiles {
		filepath := filepath.Join(client.BaseDir, filename)
		f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		check_err(err)
		hashList := remoteMetaMap[filename].BlockHashList

		// get block store address map
		blockStoreMap := make(map[string][]string)
		err = client.GetBlockStoreMap(hashList, &blockStoreMap)
		check_err(err)
		// build hash address map
		hashAddressMap := make(map[string]string)
		for addr, hashList := range blockStoreMap {
			for _, hash := range hashList {
				hashAddressMap[hash] = addr
			}
		}

		for _, hash := range hashList {
			block := Block{}
			blockAddress := hashAddressMap[hash]
			err := client.GetBlock(hash, blockAddress, &block)
			check_err(err)

			_, err = f.Write(block.BlockData)
			check_err(err)
		}
		f.Close()
	}

	// download modified files
	for _, filename := range remoteModifiedFiles {
		hashList := remoteMetaMap[filename].BlockHashList
		filepath := filepath.Join(client.BaseDir, filename)
		if CompareHashList(hashList, []string{"0"}) {
			err := os.Remove(filepath)
			check_err(err)
		} else {
			f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			check_err(err)

			hashList := remoteMetaMap[filename].BlockHashList
			// get block store address map
			blockStoreMap := make(map[string][]string)
			err = client.GetBlockStoreMap(hashList, &blockStoreMap)
			check_err(err)
			// build hash address map
			hashAddressMap := make(map[string]string)
			for addr, hashList := range blockStoreMap {
				for _, hash := range hashList {
					hashAddressMap[hash] = addr
				}
			}

			for _, hash := range hashList {
				block := Block{}
				blockAddress := hashAddressMap[hash]
				err := client.GetBlock(hash, blockAddress, &block)
				check_err(err)
				_, err = f.Write(block.BlockData)
				check_err(err)
			}
			f.Close()
		}
	}
}

func check_err(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
