
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
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
	PutToDoMap map[string]string 
	KeySet map[string]bool
	IsAborted bool
	IsCommited bool
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
		fmt.Println("  --Transaction ID:", tx.ID, "IsAborted:", tx.IsAborted, "IsCommited:", tx.IsCommited)
		fmt.Println("    KeySet:", getKeySetSlice(tx))
		fmt.Println("    PutList:")
		for k := range tx.PutToDoMap {
			fmt.Println("      Key:", k, "Value:", tx.PutToDoMap[k])
		}
	}
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
	} else if transactions[req.TxID].IsCommited {
		*resp = PutResponse{false, errors.New("Transaction is already commited")}
	} else {
		canAccess, trId := canAccessKey(req.Key, req.TxID)
		for  !canAccess {
			if isDeadlock(req.TxID, trId) {
				isAbort := resolveDeadLock(req.TxID, trId)
				if isAbort {
					abort(req.TxID)
					*resp = PutResponse{false, errors.New("Transaction is aborted")}
					return nil 
				}	 	
			} else {
				// lock this data structure
				// and take self out when not waiting.
				addToWaitingMap(req.TxID, trId)
			}
			time.Sleep(time.Millisecond * 100)
			canAccess, trId = canAccessKey(req.Key, req.TxID)
		}
		removeFromWaitingMap(req.TxID)
		setPutTransactionRecord(req)
		*resp = PutResponse{true, nil}
	}
	printState()
	return nil
}

// TODO implement
func removeFromWaitingMap(txId int) {
}

// TODO implement
func addToWaitingMap(myId int, waitingForId int) {
}

// TODO remove from waitingMap
// TODO remove KeySet??
func abort(txId int) {
	tx := transactions[txId]
	tx.IsAborted = true
	transactions[txId] = tx
}


// returns true if myId should abort, otherwise couses otherId to abort
func resolveDeadLock(myId int, otherId int) bool {
	return false
}

// TODO
func isDeadlock(myId int, otherId int) bool {
	return false
}

func canAccessKey(key string, myId int) (bool, int) {
	tx := transactions[myId]
	_, ok := tx.KeySet[key] 
	if ok {
		return true, 0
	} else {
		for k := range transactions {
			tr:= transactions[k]
			_, ok = tr.KeySet[key]
			if ok {
				return false, tr.ID	
			}
		}
		return true, 0
	}
}

func setPutTransactionRecord(req PutRequest) {
	tx := transactions[req.TxID]
	tx.PutToDoMap[req.Key] = req.Value 
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
	tx := Transaction{tID, make(map[string]string), make(map[string]bool), false, false}
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
