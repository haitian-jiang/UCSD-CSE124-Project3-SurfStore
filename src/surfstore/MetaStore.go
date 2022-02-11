package surfstore

import (
	"fmt"
)

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
	// Add additional data structure(s) to maintain BlockStore addresses
	BlockStoreAddr []string
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	*serverFileInfoMap = m.FileMetaMap
	*_ignore = true // todo: why
	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	filename := fileMetaData.Filename
	currFileMetaData, ok := m.FileMetaMap[filename]

	// new file
	if !ok {
		// version number is 1 in this case
		m.FileMetaMap[filename] = *fileMetaData
		*latestVersion = fileMetaData.Version
		return nil
	}

	// previously existed file
	if fileMetaData.Version != currFileMetaData.Version+1 {
		*latestVersion = currFileMetaData.Version
		return fmt.Errorf("Invalid version: current version on server is %d", currFileMetaData.Version)
	} else {
		*latestVersion = fileMetaData.Version
		m.FileMetaMap[filename] = *fileMetaData
		return nil
	}

}

func (m *MetaStore) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	*blockStoreMap = map[string][]string{m.BlockStoreAddr[0]: blockHashesIn}
	return nil
}

var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreList []string) MetaStore {
	return MetaStore{map[string]FileMetaData{}, blockStoreList}
}
