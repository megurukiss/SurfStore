package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	hash := blockHash.GetHash()
	block, ok := bs.BlockMap[hash]
	if !ok {
		log.Printf("BlockStore: Block with hash %s not found\n", hash)
		return nil, fmt.Errorf("Block with hash %s not found", hash)
	}
	return &Block{BlockData: block.GetBlockData(), BlockSize: block.GetBlockSize()}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hash] = block
	log.Printf("PutBlock: %v\n", hash)
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashes := blockHashesIn.GetHashes()
	missingHashes := []string{}
	for _, hash := range hashes {
		if _, ok := bs.BlockMap[hash]; !ok {
			missingHashes = append(missingHashes, hash)
		}
	}
	log.Printf("MissingBlocks: %v\n", missingHashes)
	return &BlockHashes{Hashes: missingHashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := []string{}
	for hash := range bs.BlockMap {
		blockHashes = append(blockHashes, hash)
	}
	return &BlockHashes{Hashes: blockHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
