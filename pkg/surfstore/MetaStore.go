package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
	// panic("todo")
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {

	givenFile := fileMetaData

	existingFileMetadata, ok := m.FileMetaMap[givenFile.Filename]

	if !ok {

		// File doesnt exist

		m.FileMetaMap[givenFile.Filename] = &FileMetaData{
			Filename:      givenFile.Filename,
			Version:       1,
			BlockHashList: givenFile.BlockHashList}

		return &Version{Version: givenFile.Version}, nil

	} else {

		// File exists with version number

		if givenFile.Version == (existingFileMetadata.Version + 1) {

			// Update the fileinfo map
			m.FileMetaMap[givenFile.Filename] = &FileMetaData{
				Filename:      givenFile.Filename,
				Version:       givenFile.Version,
				BlockHashList: givenFile.BlockHashList}

			return &Version{Version: givenFile.Version}, nil
		} else {
			return &Version{Version: -1}, fmt.Errorf("older version detected")
		}
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {

	blockServerToblockhashes := make(map[string][]string)
	blockstoremap := make(map[string]*BlockHashes)

	for _, hash := range blockHashesIn.Hashes {
		serverForthisHash := m.ConsistentHashRing.GetResponsibleServer(hash)
		val, ok := blockServerToblockhashes[serverForthisHash]

		if !ok {
			blockServerToblockhashes[serverForthisHash] = []string{hash}
		} else {
			val = append(val, hash)
			blockServerToblockhashes[serverForthisHash] = val
		}
	}

	for key, value := range blockServerToblockhashes {
		blockstoremap[key] = &BlockHashes{Hashes: value}
	}

	return &BlockStoreMap{BlockStoreMap: blockstoremap}, nil
	// panic("todo")
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {

	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
	// panic("todo")
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
