package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	currFileHashes, err := GetFileHashBlock(client)
	handle(err)

	// generate current local index
	localFileMetaMap, err := GenLocalFileMetaMap(currFileHashes, client.BaseDir)
	handle(err)
	PrintMetaMap(localFileMetaMap)

	// get remote index
	var succ bool
	var remoteFileMetaMap map[string]FileMetaData
	err = client.GetFileInfoMap(&succ, &remoteFileMetaMap)
	handle(err)
	PrintMetaMap(remoteFileMetaMap)

}

func handle(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func GetFileHashBlock(client RPCClient) (hashLists map[string][]string, err error) {
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
			return nil, errors.New("There should not be any directory in basedir.")
		}
		hashLists[file.Name()], err = CalcFileHash(file.Name(), client.BlockSize)
		if err != nil {
			return nil, err
		}
	}
	return hashLists, nil
}

func CalcFileHash(filename string, blkSize int) (hashList []string, err error) {
	f, _ := os.Open(filename)
	defer f.Close()
	reader := bufio.NewReader(f)
	buf := make([]byte, blkSize)
	for {
		size, err := reader.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		hashBytes := sha256.Sum256(buf[:size])
		hashString := hex.EncodeToString(hashBytes[:])
		hashList = append(hashList, hashString)
	}
	return hashList, nil
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
		f, _ := os.Open("index.txt")
		reader := bufio.NewScanner(f)
		for reader.Scan() {
			line := strings.Split(reader.Text(), ",")
			filename := line[0]
			version, _ := strconv.Atoi(line[1])
			hashList := strings.Split(line[3], " ")
			fileMetaMap[filename] = FileMetaData{filename, version, hashList}
		}
		f.Close()

		// new file or updated file
		for filename, currHashList := range fileHashes {
			fmd, ok := fileMetaMap[filename]
			if !ok { // untracked new file
				fileMetaMap[filename] = FileMetaData{filename, 1, currHashList}
			} else if !isEqualHash(fmd.BlockHashList, currHashList) {
				fileMetaMap[filename] = FileMetaData{filename, fmd.Version + 1, currHashList}
			}
		}

		// deleted file
		for filename, _ := range fileMetaMap {
			_, ok := fileHashes[filename]
			if !ok {
				delete(fileMetaMap, filename)
			}
		}
		return fileMetaMap, nil
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
