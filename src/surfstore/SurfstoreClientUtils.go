package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const INDEX_FILE = "index.txt"

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	// Get current hash list
	currFileHashes, err := GetFileHashLists(client)
	handle(err)
	// generate current local index
	localFileMetaMap, err := GenLocalFileMetaMap(currFileHashes, client.BaseDir)
	handle(err)

	// get remote index
	var succ bool
	var remoteFileMetaMap map[string]FileMetaData
	err = client.GetFileInfoMap(&succ, &remoteFileMetaMap)
	handle(err)

	// merge and update
	// new file from server
	for filename, rmtFMD := range remoteFileMetaMap {
		locFMD, ok := localFileMetaMap[filename]
		if !ok || rmtFMD.Version > locFMD.Version {
			DownloadFileUpdateLocal(client, filename, &localFileMetaMap, &remoteFileMetaMap)
		}
	}
	// new file from local
	for filename, locFMD := range localFileMetaMap {
		rmtFMD, ok := remoteFileMetaMap[filename]
		if !ok || locFMD.Version == rmtFMD.Version+1 {
			UpdateFileToServer(client, filename, &localFileMetaMap)
		} else if locFMD.Version > rmtFMD.Version+1 { // normally impossible
			log.Printf("Wrong version for %s", filename)
			os.Exit(127)
		} else if !isEqualHash(locFMD.BlockHashList, rmtFMD.BlockHashList) { // same version number, conflict
			DownloadFileUpdateLocal(client, filename, &localFileMetaMap, &remoteFileMetaMap)
		}
	}

	// write local file meta map back to index.txt
	UpdateLocalIndex(client.BaseDir+"/index.txt", &localFileMetaMap)
}

func handle(err error) {
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func isEqualHash(hashList1 []string, hashList2 []string) bool {
	if len(hashList1) != len(hashList2) {
		return false
	}
	for i := 0; i < len(hashList1); i++ {
		if hashList1[i] != hashList2[i] {
			return false
		}
	}
	return true
}

func isDeleted(fileMetaData FileMetaData) bool {
	if len(fileMetaData.BlockHashList) == 0 {
		return false
	}
	if fileMetaData.BlockHashList[0] == "0" {
		return true
	}
	return false
}

func GetFileHashLists(client RPCClient) (hashLists map[string][]string, err error) {
	// read directory
	dir, err := os.ReadDir(client.BaseDir)
	if err != nil {
		return nil, err
	}
	hashLists = make(map[string][]string)

	// calculate hash
	for _, file := range dir {
		if file.Name() == INDEX_FILE {
			continue
		}
		if file.IsDir() {
			return nil, errors.New("There should not be any directory in basedir")
		}
		hashLists[file.Name()], _, err = CalcFileHashAndBlock(client.BaseDir+"/"+file.Name(), client.BlockSize, false)
		if err != nil {
			return nil, err
		}
	}
	return hashLists, nil
}

func CalcFileHashAndBlock(filePath string, blkSize int, needBlock bool) (hashList []string, blockMap map[string]Block, err error) {
	f, _ := os.Open(filePath)
	defer f.Close()
	reader := bufio.NewReader(f)
	buf := make([]byte, blkSize)
	blockMap = make(map[string]Block)
	for {
		// read bytes
		size, err := reader.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		// calculate hash
		hashBytes := sha256.Sum256(buf[:size])
		hashString := hex.EncodeToString(hashBytes[:])
		hashList = append(hashList, hashString)
		// store block
		if needBlock {
			blockMap[hashString] = Block{buf[:size], size}
		}
	}
	if needBlock {
		return hashList, blockMap, nil
	} else {
		return hashList, nil, nil
	}
}

func GenLocalFileMetaMap(fileHashes map[string][]string, baseDir string) (map[string]FileMetaData, error) {
	fileMetaMap := make(map[string]FileMetaData)

	_, err := os.Stat(baseDir + "/index.txt")

	if os.IsNotExist(err) {
		for filename, hashList := range fileHashes {
			fileMetaMap[filename] = FileMetaData{filename, 1, hashList}
		}
		return fileMetaMap, nil
	} else if err != nil {
		return nil, err
	} else { // index.txt exists
		f, _ := os.Open(baseDir + "/index.txt")
		reader := bufio.NewScanner(f)
		for reader.Scan() {
			line := strings.Split(reader.Text(), ",")
			filename := line[0]
			version, _ := strconv.Atoi(line[1])
			hashList := strings.Split(line[2], " ")
			if hashList[0] == "" {
				hashList = nil // empty file
			}
			fileMetaMap[filename] = FileMetaData{filename, version, hashList}
		}
		f.Close()

		// new file or updated file, recreated deleted file counts as updated file
		for filename, currHashList := range fileHashes {
			fmd, ok := fileMetaMap[filename]
			if !ok { // untracked new file
				fileMetaMap[filename] = FileMetaData{filename, 1, currHashList}
			} else if !isEqualHash(fmd.BlockHashList, currHashList) {
				fileMetaMap[filename] = FileMetaData{filename, fmd.Version + 1, currHashList}
			}
		}

		// deleted file
		for filename := range fileMetaMap {
			if _, ok := fileHashes[filename]; !ok { // tombstone status
				newVersion := fileMetaMap[filename].Version + 1
				fileMetaMap[filename] = FileMetaData{filename, newVersion, []string{"0"}}
			}
		}
		return fileMetaMap, nil
	}
}

func DownloadFileUpdateLocal(client RPCClient, filename string, locFMM *map[string]FileMetaData, rmtFMM *map[string]FileMetaData) {
	// update local file meta map
	(*locFMM)[filename] = (*rmtFMM)[filename]

	// no block to download for deleted files
	if isDeleted((*rmtFMM)[filename]) {
		// delete local file if exists
		_, err := os.Stat(client.BaseDir + "/" + filename)
		if err == nil {
			os.Remove(client.BaseDir + "/" + filename)
		}
		return
	}

	// obtain blockstore server address
	fileHashList := (*rmtFMM)[filename].BlockHashList
	var blockStoreMap map[string][]string
	client.GetBlockStoreMap(fileHashList, &blockStoreMap)

	// download blocks
	fileBlockMap := make(map[string]Block)
	for blockAddr, blockList := range blockStoreMap {
		for _, hash := range blockList {
			var block Block
			client.GetBlock(hash, blockAddr, &block)
			fileBlockMap[hash] = block
		}
	}

	// reconstruct file
	var fileContent []byte
	for _, hash := range fileHashList {
		fileContent = append(fileContent, fileBlockMap[hash].BlockData...)
	}

	// store file
	os.WriteFile(client.BaseDir+"/"+filename, fileContent, 0640)
}

func UpdateFileToServer(client RPCClient, filename string, locFMM *map[string]FileMetaData) {
	fmd := (*locFMM)[filename]

	// get blockstore server address
	var blockStoreMap map[string][]string
	client.GetBlockStoreMap(fmd.BlockHashList, &blockStoreMap)
	var blockAddr string
	for key := range blockStoreMap {
		blockAddr = key
	}

	// get list of blocks needed to be uploaded
	hasBlockHashes := make(map[string]bool)
	client.HasBlocks(fmd.BlockHashList, blockAddr, &hasBlockHashes)

	// upload blocks
	_, blocks, _ := CalcFileHashAndBlock(client.BaseDir+"/"+filename, client.BlockSize, true)
	for _, hash := range fmd.BlockHashList {
		if _, ok := hasBlockHashes[hash]; !ok {
			var succ bool
			client.PutBlock(blocks[hash], blockAddr, &succ)
		}
	}

	// update meta data
	var latestVer int
	err := client.UpdateFile(&fmd, &latestVer)
	if err != nil {
		log.Println(err)
	}
}

func UpdateLocalIndex(indexPath string, locFMM *map[string]FileMetaData) {
	var indexContent string
	for filename, fmd := range *locFMM {
		hash := fmt.Sprintf("%v", fmd.BlockHashList)
		line := fmt.Sprintf("%s,%d,%s\n", filename, fmd.Version, hash[1:len(hash)-1])
		indexContent += line
	}
	os.WriteFile(indexPath, []byte(indexContent), 0640)
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, fileMeta := range metaMap {
		fmt.Println("\t", fileMeta.Filename, fileMeta.Version, fileMeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}
