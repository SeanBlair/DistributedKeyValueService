
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
	// "net/url"
	// "errors"
	"os"
	"strconv"
	// "strings"
	"sync"
)

var (
	nodesFile string
	nodeID int
	listenNodeIpPort string
	listenClientIpPort string
	transactions map[int]Transaction
	nextTransactionId int
	nextGlobalCommitId int
	theValueStore map[string]string
	// stores txId (key) is waiting for txId (val)
	waitingMap map[int]int
	mutex *sync.Mutex
)



type Transaction struct {
	ID int
	PutToDoMap map[string]string 
	KeySet map[string]bool
	IsAborted bool
	IsCommitted bool
}

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

type KVServer int

type NewTransactionResp struct {
	TxID int
}

type PutRequest struct {
	TxID int
	Key Key
	Value Value
}


type PutResponse struct {
	Success bool
	Err string
}

type GetRequest struct {
	TxID int
	Key Key
}

type GetResponse struct {
	Success bool
	Value Value
	Err string
}

type CommitRequest struct {
	TxID int
}

type CommitResponse struct {
	Success bool
	CommitId int
	Err string
}

type AbortRequest struct {
	TxID int
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
	nextGlobalCommitId = 1
	transactions = make(map[int]Transaction)
	theValueStore = make(map[string]string)
	waitingMap = make(map[int]int)
	mutex = &sync.Mutex{}

	printState()

	listenClients()
}

func printState() {
	fmt.Println("\nKVNODE STATE:")
	fmt.Println("-TheValueStore:")
	
	mutex.Lock()
	for k := range theValueStore {
		fmt.Println("    Key:", k, "Value:", theValueStore[k])
	}
	mutex.Unlock()

	fmt.Println("-Transactions:")
	mutex.Lock()
	for txId := range transactions {
		tx := transactions[txId]
		fmt.Println("  --Transaction ID:", tx.ID, "IsAborted:", tx.IsAborted, "IsCommitted:", tx.IsCommitted)
		fmt.Println("    KeySet:", getKeySetSlice(tx))
		fmt.Println("    PutToDoMap:")
		for k := range tx.PutToDoMap {
			fmt.Println("      Key:", k, "Value:", tx.PutToDoMap[k])
		}
	}
	fmt.Println("Total number of transactions is:", len(transactions))
	mutex.Unlock()
}

func getKeySetSlice(tx Transaction) (keySetString []string) {
	keySet := tx.KeySet
	for key := range keySet {
		keySetString = append(keySetString, key)
	}
	return
}

func (p *KVServer) Abort(req AbortRequest, resp *bool) error {
	fmt.Println("\n Received a call to Abort")
	abort(req.TxID)
	removeFromWaitingMap(req.TxID)
	*resp = true
	printState()
	return nil
}

func (p *KVServer) Commit(req CommitRequest, resp *CommitResponse) error {
	fmt.Println("\n Received a call to Commit")
	mutex.Lock()
	tx := transactions[req.TxID]
	mutex.Unlock()
	if tx.IsAborted {
		*resp = CommitResponse{false, 0, "Transaction is already aborted"}
	} else if tx.IsCommitted {
		*resp = CommitResponse{false, 0, "Transaction is already commited"}
	} else {
		toDo := tx.PutToDoMap

		mutex.Lock()
		for k := range toDo {
			theValueStore[k] = toDo[k]
		}
		mutex.Unlock()

		tx.PutToDoMap = make(map[string]string)
		tx.KeySet = make(map[string]bool)
		tx.IsCommitted = true
		mutex.Lock()
		transactions[req.TxID] = tx
		mutex.Unlock()
		*resp = CommitResponse{true, nextGlobalCommitId, ""}
		nextGlobalCommitId++
	}
	printState()
	return nil
}

func (p *KVServer) Get(req GetRequest, resp *GetResponse) error {
	fmt.Println("\nReceived a call to Get()")
	mutex.Lock()
	tx := transactions[req.TxID]
	mutex.Unlock()
	var returnVal Value 
	if tx.IsAborted {
		*resp = GetResponse{false, returnVal, "Transaction is already aborted"}
	} else if tx.IsCommitted {
		*resp = GetResponse{false, returnVal, "Transaction is already committed"}
	} else {
		canAccess, trId := canAccessKey(string(req.Key), req.TxID)
		for  !canAccess {
			var ids []int
			if isDeadlock(req.TxID, trId, &ids)  {
				isAbort := resolveDeadLock(req.TxID, ids)
				if isAbort {
					removeFromWaitingMap(req.TxID)
					*resp = GetResponse{false, returnVal, "Transaction was aborted"}
					printState()
					return nil 
				}	 	
			} else {
				addToWaitingMap(req.TxID, trId)
			}
			time.Sleep(time.Millisecond * 10)
			// Other transaction aborted me
			mutex.Lock()
			imAborted := transactions[req.TxID].IsAborted
			mutex.Unlock()
			if imAborted {
				removeFromWaitingMap(req.TxID)
				*resp = GetResponse{false, returnVal, "Transaction was aborted"}
				printState()
				return nil
			}
			// Reset loop guard
			canAccess, trId = canAccessKey(string(req.Key), req.TxID)
		}
		// Happy path
		removeFromWaitingMap(req.TxID)
		updateKeySet(req.TxID, string(req.Key))
		returnVal = getValue(req.TxID, string(req.Key))
		*resp = GetResponse{true,  returnVal, ""}
	}
	printState()
	return nil
}

// Return value for key, either from transaction PutToDoMap
// or from theValueStore map. Should only ever be called by
// transaction that has id == tid
func getValue(tid int, key string) Value {
	mutex.Lock()
	val, ok := transactions[tid].PutToDoMap[key]
	mutex.Unlock()
	if ok {
		return Value(val)
	} else {
		mutex.Lock()
		v := theValueStore[key]
		mutex.Unlock()
		return Value(v)
	}
}

func updateKeySet(tid int, key string) {
	mutex.Lock()
	transactions[tid].KeySet[key] = true
	mutex.Unlock()
}

func (p *KVServer) Put(req PutRequest, resp *PutResponse) error {
	fmt.Println("\nReceived a call to Put()")
	mutex.Lock()
	tx := transactions[req.TxID]
	mutex.Unlock()
	if tx.IsAborted {
		*resp = PutResponse{false, "Transaction is already aborted"}
	} else if tx.IsCommitted {
		*resp = PutResponse{false, "Transaction is already commited"}
	} else {
		// true if no transaction owns req.Key
		canAccess, trId := canAccessKey(string(req.Key), req.TxID)
		for  !canAccess {
			fmt.Println("transactionID:", req.TxID , "Can't Access!! Key:", req.Key, "owned by:", trId)
			var ids []int
			// There is a cycle starting at trId and ending at me
			if isDeadlock(req.TxID, trId, &ids)  {
				// aborts correct transaction
				isAbort := resolveDeadLock(req.TxID, ids)
				// I was aborted
				if isAbort {
					removeFromWaitingMap(req.TxID)
					*resp = PutResponse{false, "Transaction was aborted"}
					printState()
					return nil 
				}	 	
			} else {
				addToWaitingMap(req.TxID, trId)
			}
			time.Sleep(time.Millisecond * 100)
			// Other transaction aborted me
			mutex.Lock()
			imAborted := transactions[req.TxID].IsAborted
			mutex.Unlock()
			if imAborted {
				removeFromWaitingMap(req.TxID)
				*resp = PutResponse{false, "Transaction was aborted"}
				printState()
				return nil
			}
			// Reset loop guard
			canAccess, trId = canAccessKey(string(req.Key), req.TxID)
		}
		// Happy path
		removeFromWaitingMap(req.TxID)
		setPutTransactionRecord(req)
		*resp = PutResponse{true, ""}
	}
	printState()
	return nil
}

// TODO implement
func removeFromWaitingMap(txId int) {
	mutex.Lock()
	delete(waitingMap, txId)
	mutex.Unlock()
	fmt.Println("WaitingMap after deleting id:", txId)
}

// TODO implement
func addToWaitingMap(myId int, waitingForId int) {
	mutex.Lock()
	waitingMap[myId] = waitingForId
	mutex.Unlock()
	fmt.Println("WaitingMap after adding id:", myId)
}


func abort(txId int) {
	mutex.Lock()
	tx := transactions[txId]
	tx.IsAborted = true
	tx.PutToDoMap = make(map[string]string)
	tx.KeySet = make(map[string]bool)
	transactions[txId] = tx
	mutex.Unlock()
}


// returns true if myId aborted, returns false if otherId aborted
func resolveDeadLock(myId int, otherIds []int) (isAbort bool) {
	isAbort = true
	txWithMinKeySet := myId
	mutex.Lock()
	keySet := transactions[myId].KeySet
	mutex.Unlock()
	minKeys := len(keySet)
	for _, id := range otherIds {
		mutex.Lock()
		keySet = transactions[id].KeySet
		mutex.Unlock()
		if len(keySet) < minKeys {
			minKeys = len(keySet)
			txWithMinKeySet = id
			isAbort = false
		}
	} 
	abort(txWithMinKeySet)
	return
}

// TODO Lock??
// Returns true if otherId is waiting for myId
func isDeadlock(myId int, otherId int, idsInDeadLock *[]int) bool {
	mutex.Lock()
	_, ok := waitingMap[otherId]
	mutex.Unlock()
	// otherId not in map therefore not waiting for myId
	if !ok {
		return false
	} else {
		*idsInDeadLock = append(*idsInDeadLock, otherId)
		// waiting for me (deadLock!)
		mutex.Lock()
		otherIdsBlocker := waitingMap[otherId]
		mutex.Unlock()
		if otherIdsBlocker == myId {
			return true
		} else {
			// check if tx otherId is waiting for is waiting for myId
			return isDeadlock(myId, waitingMap[otherId], idsInDeadLock)
		}
	}
	return false
}


func canAccessKey(key string, myId int) (bool, int) {
	mutex.Lock()
	tx := transactions[myId]
	mutex.Unlock()
	_, ok := tx.KeySet[key] 
	if ok {
		return true, 0
	} else {
		mutex.Lock()
		for k := range transactions {
			tr:= transactions[k]
			_, ok = tr.KeySet[key]
			if ok && !tr.IsAborted && !tr.IsCommitted {
				return false, tr.ID	
			}
		}
		mutex.Unlock()
		return true, 0
	}
}

func setPutTransactionRecord(req PutRequest) {
	mutex.Lock()
	transactions[req.TxID].PutToDoMap[string(req.Key)] = string(req.Value)
	transactions[req.TxID].KeySet[string(req.Key)] = true
	mutex.Unlock()
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
	mutex.Lock()
	transactions[tID] = tx
	mutex.Unlock()
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
		go kvServer.ServeConn(conn)
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
