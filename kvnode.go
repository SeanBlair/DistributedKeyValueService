
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	// "net/url"
	"errors"
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
	theValueStore map[string]string
)



type Transaction struct {
	ID int
	// TODO make this a set (map), only need to undo the first Put in a transaction
	PutList []Put
	KeySet map[string]bool
	IsAborted bool
	IsCommited bool
}

type Put struct {
	Key string
	Value string
	PreviousValue string
	IsNewKey bool
}

type KVServer int

type NewTransactionResp struct {
	ID int
}

type PutRequest struct {
	TxID int
	Key string
	Value string
}

type PutResponse struct {
	Success bool
	Err error 
}

func main() {
	err := ParseArguments()
	if err != nil {
		panic(err)
	}
	fmt.Println("Command line arguments are: nodesFile:", nodesFile, "nodeID:", nodeID, 
		"listenNodeIpPort", listenNodeIpPort, "listenClientIpPort", listenClientIpPort)

	fmt.Println("KVNode with id:", nodeID, "is Alive!!")

	nextTransactionId = 1
	transactions = make(map[int]Transaction)
	theValueStore = make(map[string]string)

	printState()

	listenClients()
}

func printState() {
	fmt.Println("\nKVNODE STATE:")
	fmt.Println("-TheValueStore:")
	for k := range theValueStore {
		fmt.Println("    Key:", k, "Value:", theValueStore[k])
	}
	fmt.Println("-Transactions:")
	for txId := range transactions {
		tx := transactions[txId]
		fmt.Println("  --Transaction ID:", tx.ID, "IsAborted:", tx.IsAborted, "IsCommited", tx.IsCommited)
		fmt.Println("    KeySet:", getKeySetSlice(tx))
		fmt.Println("    PutList:")
		for _, put := range tx.PutList {
			fmt.Println("      ", getPutString(put), put.IsNewKey)
		}
	}
}

func getPutString(put Put) string {
	return "Key:" + put.Key + " Value:" + put.Value + " PreviousValue:" + 
	put.PreviousValue + " IsNewKey:"
}

func getKeySetSlice(tx Transaction) (keySetString []string) {
	keySet := tx.KeySet
	for key := range keySet {
		keySetString = append(keySetString, key)
	}
	return
}


func (p *KVServer) Put(req PutRequest, resp *PutResponse) error {
	fmt.Println("\nReceived a call to Put()")

	if transactions[req.TxID].IsAborted {
		*resp = PutResponse{false, errors.New("Transaction is already aborted")}

	} else {
		setTransactionRecord(req)

		theValueStore[req.Key] = req.Value

		*resp = PutResponse{true, nil}
	}
	printState()
	return nil
}

func setTransactionRecord(req PutRequest) {
	put := Put{}
	if isKeyInStore(req.Key) {
		put.IsNewKey = false
		put.PreviousValue = theValueStore[req.Key]
	} else {
		put.IsNewKey = true
	}
	put.Key = req.Key
	put.Value = req.Value

	tx := transactions[req.TxID]

	tx.PutList = append(tx.PutList, put)
	tx.KeySet[req.Key] = true

	transactions[req.TxID] = tx
}

func isKeyInStore(k string) bool {
	_, ok := theValueStore[k]
	return ok
}

func (p *KVServer) NewTransaction(req bool, resp *NewTransactionResp) error {
	fmt.Println("\nReceived a call to NewTransaction()")
	tID := nextTransactionId
	nextTransactionId++
	var putList []Put 
	tx := Transaction{tID, putList, make(map[string]bool), false, false}
	transactions[tID] = tx
	*resp = NewTransactionResp{tID}
	printState()
	return nil
}

func listenClients() {
	kvServer := rpc.NewServer()
	kv := new(KVServer)
	kvServer.Register(kv)
	l, err := net.Listen("tcp", listenClientIpPort)
	checkError("Error in listenClients(), net.Listen()", err, true)
	fmt.Println("Listening for client rpc calls on:", listenClientIpPort)
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
