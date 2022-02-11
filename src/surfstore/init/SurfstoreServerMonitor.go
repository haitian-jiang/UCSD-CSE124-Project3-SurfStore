package main

import (
	"fmt"
	"surfstore"
)

func main() {
	rpcClient := surfstore.NewSurfstoreRPCClient("localhost:8080", "", 0)
	var succ bool
	var serverMap map[string]surfstore.FileMetaData
	rpcClient.GetFileInfoMap(&succ, &serverMap)
	surfstore.PrintMetaMap(serverMap)
	var block surfstore.Block
	rpcClient.GetBlock("0", "localhost:8080", &block)
	fmt.Println(block)
}
