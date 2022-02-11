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
	rpcClient.GetBlock("ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7", "localhost:8080", &block)
	fmt.Println(block)
}
