
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	// "net/url"
	"os"
	"strconv"
	// "strings"
)

var (
	nodesFile string
	nodeID int
	listenNodeIpPort string
	listenClientIpPort string
	transactions map[int]Transaction
	nextTransactionId int
)

type Transaction struct {
	ID int
	PutList []Put
	KeySet map[string]bool
	isAborted bool
	isCommited bool
}

type Put struct {
	Key string
	Value string
	PreviousValue string
	isNewKey bool
}

func main() {
	err := ParseArguments()
	if err != nil {
		panic(err)
	}
	fmt.Println("nodesFile:", nodesFile, "nodeID:", nodeID, "listenNodeIpPort", listenNodeIpPort, "listenClientIpPort", listenClientIpPort)

	nextTransactionId = 1
	transactions = make(map[int]Transaction)

	listenClients()
}

type KVServer int

type NewTransactionResp struct {
	ID int
}

func (p *KVServer) NewTransaction(req bool, resp *NewTransactionResp) error {
	fmt.Println("Received a call to NewTransaction()")
	tID := nextTransactionId
	nextTransactionId++
	var putList []Put 
	tx := Transaction{tID, putList, make(map[string]bool), false, false}
	transactions[tID] = tx
	*resp = NewTransactionResp{tID}
	return nil
}

func listenClients() {
	kvServer := rpc.NewServer()
	kv := new(KVServer)
	kvServer.Register(kv)
	l, err := net.Listen("tcp", listenClientIpPort)
	checkError("Error in listenClients(), net.Listen()", err, true)
	fmt.Println("listening for rpc calls on:", listenClientIpPort)
	for {
		conn, err := l.Accept()
		checkError("Error in listenClients(), l.Accept()", err, true)
		kvServer.ServeConn(conn)
	}
}

// Parses the command line arguments to server.go
func ParseArguments() (err error) {
	arguments := os.Args[1:]
	if len(arguments) == 4 {
		nodesFile = arguments[0]
		nodeID, err = strconv.Atoi(arguments[1])
		checkError("Error in ParseArguments(), strconv.Atoi()", err, true)
		listenNodeIpPort = arguments[2]
		listenClientIpPort = arguments[3]
	} else {
		err = fmt.Errorf("Usage: {go run kvnode.go [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]}")
		return
	}
	return
}


// Prints msg + err to console and exits program if exit == true
func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
