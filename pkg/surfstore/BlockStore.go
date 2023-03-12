package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {

	val, ok := bs.BlockMap[blockHash.Hash]

	if ok {
		return &Block{BlockData: []byte(val.BlockData), BlockSize: int32(val.BlockSize)}, nil
	} else {
		return &Block{BlockData: []byte{}, BlockSize: -1}, fmt.Errorf("no value found")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {

	blockHash := GetBlockHashString(block.BlockData)

	bs.BlockMap[blockHash] = &Block{BlockData: block.BlockData, BlockSize: block.BlockSize}

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {

	hashlist_in := blockHashesIn.Hashes

	var hashlist_out []string

	for _, hash := range hashlist_in {
		_, ok := bs.BlockMap[hash]

		if ok {
			hashlist_out = append(hashlist_out, hash)
		}
	}

	return &BlockHashes{Hashes: hashlist_out}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {

	var blockHashlist []string
	for key := range bs.BlockMap {
		blockHashlist = append(blockHashlist, key)
	}

	return &BlockHashes{Hashes: blockHashlist}, nil
	// panic("todo")
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
