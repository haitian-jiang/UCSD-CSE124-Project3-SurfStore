package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]Block
}

func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	block, ok := bs.BlockMap[blockHash]
	if !ok {
		return errors.New("Block Not Exist!")
	}
	*blockData = block
	return nil
}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	hashBytes := sha256.Sum256(block.BlockData)
	hashString := hex.EncodeToString(hashBytes[:])
	bs.BlockMap[hashString] = block
	*succ = true
	return nil
}

func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *map[string]bool) error {
	for _, hash := range blockHashesIn {
		if _, ok := bs.BlockMap[hash]; ok {
			(*blockHashesOut)[hash] = true
		}
	}
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() BlockStore {
	return BlockStore{BlockMap: map[string]Block{}}
}
