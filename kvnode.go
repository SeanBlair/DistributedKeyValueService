package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
	// "net/url"
	// "errors"
	"bufio"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	nodesFilePath string
	// maps other nodes' ip:ports to their alive status
	// kvnodesMap map[string]bool

	nodeID int

	// ipPort that other kvnodes use to talk to me
	myKvnodeIpPort     string
	listenClientIpPort string

	nextClientIsAlivePort int
	nextClientID          int

	transactions       map[int]Transaction
	nextTransactionId  int
	nextGlobalCommitId int
	theValueStore      map[string]string
	// stores txId (key) is waiting for txId (val)
	waitingMap map[int]int
	mutex      *sync.Mutex
	sleepTime  time.Duration = 100

	isWorking bool
	isLeader  bool
	kvnodes   map[int]KVNode
	nodeIds   []int

	// TODO set this to 4 when turn in code
	secondsForBootstrap time.Duration = 4

	isTimeout bool
)

type KVNode struct {
	ID       int
	IpPort   string
	IsLeader bool
	IsAlive  bool
}

type Transaction struct {
	ID          int
	ClientID    int
	KVNodeID    int
	PutToDoMap  map[string]string
	KeySet      map[string]bool
	IsAborted   bool
	IsCommitted bool
	CommitId    int
}

type UpdateStateRequest struct {
	Transactions       map[int]Transaction
	NextTransactionId  int
	NextClientID       int
	NextGlobalCommitId int
	TheValueStore      map[string]string
	WaitingMap         map[int]int
	KVNodes            map[int]KVNode
}

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

type KVServer int

type NewConnectionResp struct {
	ClientID    int
	IsAlivePort int
}

type NewTransactionReq struct {
	ClientID int
}

type NewTransactionResp struct {
	TxID int
}

type PutRequest struct {
	TxID  int
	Key   Key
	Value Value
}

type PutResponse struct {
	Success bool
	Err     string
}

type GetRequest struct {
	TxID int
	Key  Key
}

type GetResponse struct {
	Success bool
	Value   Value
	Err     string
}

type CommitRequest struct {
	TxID int
}

type CommitResponse struct {
	Success  bool
	CommitId int
	Err      string
}

type AbortRequest struct {
	TxID int
}

func main() {
	err := ParseArguments()
	if err != nil {
		panic(err)
	}
	fmt.Println("Command line arguments are: nodesFilePath:", nodesFilePath, "nodeID:", nodeID,
		"myKvnodeIpPort", myKvnodeIpPort, "listenClientIpPort", listenClientIpPort)

	fmt.Println("KVNode with id:", nodeID, "is Alive!!")

	nextTransactionId = 1
	nextGlobalCommitId = 1
	nextClientID = 1
	transactions = make(map[int]Transaction)
	theValueStore = make(map[string]string)
	waitingMap = make(map[int]int)
	mutex = &sync.Mutex{}

	setKvnodesAndNodeIds()
	isWorking = false
	isLeader = nodeID == 1
	if isLeader {
		fmt.Println("I AM THE LEADER......")
	}

	nextClientIsAlivePort = computeClientIsAlivePort()
	fmt.Println("nextClientIsAlivePort:", nextClientIsAlivePort)

	printState()

	// updateState = make(chan bool, 7)

	go listenClients()
	go listenKvNodes()
	time.Sleep(time.Second * secondsForBootstrap)
	alternateLeader()
}

func computeClientIsAlivePort() int {
	var ports []int
	listenClientPort := listenClientIpPort[(strings.Index(listenClientIpPort, ":") + 1):]
	clientPort, err := strconv.Atoi(listenClientPort)
	checkError("Error in computeClientIsAlivePort(), strconv.Atoi()", err, true)
	ports = append(ports, clientPort)
	listenKvNodesPort := myKvnodeIpPort[(strings.Index(myKvnodeIpPort, ":") + 1):]
	nodesPort, err := strconv.Atoi(listenKvNodesPort)
	checkError("Error in computeClientIsAlivePort(), strconv.Atoi()", err, true)
	ports = append(ports, nodesPort)
	sort.Ints(ports)
	largest := ports[len(ports)-1]
	return largest + 10
}

func alternateLeader() {
	for {
		// TODO find out why this avoids crashes while
		// if isLeader && !isWorking crashes....
		canPassLeadership := isLeader && !isWorking
		if canPassLeadership {
			// isWorking = true
			nextLeader := chooseNextLeader()
			fmt.Println("NEXT LEADER!!!!!!   IS:", nextLeader)
			if nextLeader != nodeID {
				fmt.Println("\nReleasing Leadership")
				mutex.Lock()
				node := kvnodes[nodeID]
				node.IsLeader = false
				kvnodes[nodeID] = node
				isLeader = false
				fmt.Println("\nI AM NOT THE LEADER     :)     :)     :)")
				node = kvnodes[nextLeader]
				node.IsLeader = true
				kvnodes[nextLeader] = node
				mutex.Unlock()
				broadcastState()
				// isWorking = false
			}
		}
		time.Sleep(time.Second)
	}
}

func chooseNextLeader() int {
	for myIndex, id := range nodeIds {
		if id == nodeID {
			for i := 1; i <= len(nodeIds); i++ {
				nextIndex := (myIndex + i) % len(nodeIds)
				nextId := nodeIds[nextIndex]
				mutex.Lock()
				kvnode := kvnodes[nextId]
				mutex.Unlock()
				if kvnode.IsAlive {
					return kvnode.ID
				}
			}
		}
	}
	return nodeID
}

func getOffsetFromLeader() (offset int) {
	offset = 1
	leaderId := getLeaderId()
	leaderIndex := leaderId - 1
	for i := 1; i <= len(nodeIds); i++ {
		nextIndex := (leaderIndex + i) % len(nodeIds)
		nextId := nodeIds[nextIndex]
		mutex.Lock()
		kvnode := kvnodes[nextId]
		mutex.Unlock()
		if kvnode.ID == nodeID {
			return
		} else if kvnode.IsAlive {
			offset++
		}
	}
	return
}

func getLeaderId() int {
	mutex.Lock()
	nodes := kvnodes
	mutex.Unlock()
	for k := range nodes {
		node := nodes[k]
		if node.IsLeader {
			return k
		}
	}
	return 0
}

func setKvnodesAndNodeIds() {
	kvnodes = make(map[int]KVNode)
	pF, err := os.Open(nodesFilePath)
	checkError("Error in setKvnodesMap(), os.Open()", err, true)
	scanner := bufio.NewScanner(pF)

	id := 1
	for scanner.Scan() {
		ipPort := scanner.Text()
		err := scanner.Err()
		checkError("Error in setKvnodesMap(), scanner.Err()", err, true)
		kvnodes[id] = KVNode{id, ipPort, id == 1, true}
		nodeIds = append(nodeIds, id)
		id++
	}
	fmt.Println("kvnodes contains:", kvnodes)
	fmt.Println("nodeIds contains:", nodeIds)
}

func printState() {
	// 	fmt.Println("\nKVNODE STATE:")
	// 	fmt.Println("-KVNodes:")
	// 	for id := range kvnodes {
	// 		mutex.Lock()
	// 		n := kvnodes[id]
	// 		mutex.Unlock()
	// 		fmt.Println("    Id:", n.ID, "IpPort:", n.IpPort, "IsLeader:", n.IsLeader, "IsAlive:", n.IsAlive)
	// 	}
	// 	fmt.Println("-TheValueStore:")
	// 	for k := range theValueStore {
	// 		mutex.Lock()
	// 		fmt.Println("    Key:", k, "Value:", theValueStore[k])
	// 		mutex.Unlock()
	// 	}
	// 	fmt.Println("-Transactions:")
	// 	for txId := range transactions {
	// 		mutex.Lock()
	// 		tx := transactions[txId]
	// 		mutex.Unlock()
	// 		fmt.Println("  --Transaction ID:", tx.ID, "ClientID:", tx.ClientID, "KVNodeID:", tx.KVNodeID,
	// 		 "IsAborted:", tx.IsAborted, "IsCommitted:", tx.IsCommitted, "CommitId:", tx.CommitId)
	// 		fmt.Println("    KeySet:", getKeySetSlice(tx))
	// 		fmt.Println("    PutToDoMap:")
	// 		for k := range tx.PutToDoMap {
	// 			fmt.Println("      Key:", k, "Value:", tx.PutToDoMap[k])
	// 		}
	// 	}
	// 	fmt.Println("Total number of transactions is:", len(transactions))
}

func getKeySetSlice(tx Transaction) (keySetString []string) {
	keySet := tx.KeySet
	for key := range keySet {
		keySetString = append(keySetString, key)
	}
	return
}

func (p *KVServer) NewConnection(req bool, resp *NewConnectionResp) error {
	fmt.Println("Received a call to NewConnection()")

	// need to have NextClientID as global state.
	if !isLeader {
		becomeLeader()
	}
	isWorking = true
	mutex.Lock()
	isAlivePort := nextClientIsAlivePort
	nextClientIsAlivePort++
	clientID := nextClientID
	nextClientID++
	mutex.Unlock()
	go monitorNewConnection(isAlivePort, clientID)
	*resp = NewConnectionResp{clientID, isAlivePort}
	printState()
	broadcastState()
	isWorking = false
	return nil
}

func monitorNewConnection(port int, clientID int) {
	ip := listenClientIpPort[:strings.Index(listenClientIpPort, ":")]
	portString := strconv.Itoa(port)
	ln, err := net.Listen("tcp", ip+":"+portString)
	checkError("Error in monitorNewConnection(), net.Listen():", err, true)
	fmt.Println("listening for connection to monitor on port:", port)
	conn, err := ln.Accept()
	fmt.Println("Accepted a connection to monitor on port:", port)

	// TODO handle error
	if err != nil {
		fixDeadClient(clientID)
	}

	for {
		// set timeout
		conn.SetReadDeadline(time.Now().Add(time.Second * 2))
		// send ping
		fmt.Fprintf(conn, "Ping")
		// listen for pong
		buffer := make([]byte, 10)
		_, err := conn.Read(buffer)
		if err, ok := err.(net.Error); ok && err.Timeout() {
			fmt.Println("detected a timeout dead client on port:", port)
			fixDeadClient(clientID)
			break
		}
		if err != nil {
			fmt.Println("detected a dead client on port:", port, "because of err:\n", err)
			fixDeadClient(clientID)
			break
		}
		// fmt.Println("Received message:", string(buffer[0:n]), "on port:", port)
	}
}

func fixDeadClient(clientID int) {
	txId, isAbort := isTransactionToAbort(clientID)
	if isAbort {
		if !isLeader {
			becomeLeader()
		}
		isWorking = true
		abort(txId)
		broadcastState()
		printState()
		isWorking = false
	}
}

func isTransactionToAbort(clientID int) (int, bool) {
	mutex.Lock()
	txs := transactions
	mutex.Unlock()

	for k := range txs {
		t := txs[k]
		if t.ClientID == clientID && t.IsCommitted == false && t.IsAborted == false {
			return k, true
		}
	}
	return 0, false
}

func (p *KVServer) UpdateState(req UpdateStateRequest, resp *bool) error {
	fmt.Println("Received a call to UpdateState()")
	mutex.Lock()
	transactions = req.Transactions
	nextTransactionId = req.NextTransactionId
	nextGlobalCommitId = req.NextGlobalCommitId
	nextClientID = req.NextClientID
	theValueStore = req.TheValueStore
	waitingMap = req.WaitingMap
	kvnodes = req.KVNodes
	mutex.Unlock()
	mutex.Lock()
	isLeader = kvnodes[nodeID].IsLeader
	mutex.Unlock()
	*resp = true
	printState()
	if isLeader {
		fmt.Println("\nI AM THE LEADER......")
	} else {
		fmt.Println("\nI AM NOT THE LEADER    :)    :)    :)")
	}
	return nil
}

func broadcastState() {
	leaderId := getLeaderId()
	for id := range kvnodes {
		mutex.Lock()
		node := kvnodes[id]
		mutex.Unlock()
		if id != nodeID && id != leaderId && node.IsAlive == true {
			err := shareState(node.IpPort)
			if err != nil {
				fixDeadKvNode(id)
				broadcastState()
				printState()
				break
			}
		}
		if isOneNodeSystem() {
			isLeader = true
		}
	}

	// time.Sleep(time.Second)
	mutex.Lock()
	leadNode := kvnodes[leaderId]
	mutex.Unlock()
	if leaderId != nodeID && leadNode.IsAlive {
		err := shareState(leadNode.IpPort)
		if err != nil {
			fixDeadKvNode(leaderId)
			broadcastState()
			printState()
		}
	}
	if isOneNodeSystem() {
		isLeader = true
	}
}

func isOneNodeSystem() bool {
	mutex.Lock()
	nodes := kvnodes
	mutex.Unlock()
	for k := range nodes {
		node := nodes[k]
		if node.IsAlive && node.ID != nodeID {
			return false
		}
	}
	return true
}

func fixDeadKvNode(id int) {
	mutex.Lock()
	nodes := kvnodes
	mutex.Unlock()

	deadNode := nodes[id]
	deadNode.IsAlive = false
	if deadNode.IsLeader {
		deadNode.IsLeader = false
		isLeader = true
		myNode := nodes[nodeID]
		myNode.IsLeader = true
		nodes[nodeID] = myNode
	}
	nodes[id] = deadNode

	mutex.Lock()
	kvnodes = nodes
	mutex.Unlock()
	mutex.Lock()
	txs := transactions
	mutex.Unlock()
	for k := range txs {
		t := txs[k]
		if t.KVNodeID == id && t.IsCommitted == false && t.IsAborted == false {
			abort(k)
		}
	}
}

func shareState(ipPort string) error {
	mutex.Lock()
	req := UpdateStateRequest{transactions, nextTransactionId, nextClientID, nextGlobalCommitId, theValueStore, waitingMap, kvnodes}
	mutex.Unlock()
	var resp bool
	client, err := rpc.Dial("tcp", ipPort)
	checkError("rpc.Dial in shareState()", err, false)
	if err != nil {
		return err
	}
	err = client.Call("KVServer.UpdateState", req, &resp)
	checkError("Error in shareState(), client.Call(KVServer.UpdateState): ", err, false)
	if err != nil {
		return err
	}
	err = client.Close()
	checkError("Error in shareState(), client.Close(): ", err, false)
	if err != nil {
		return err
	}
	return nil
}

func (p *KVServer) Abort(req AbortRequest, resp *bool) error {
	fmt.Println("\n Received a call to Abort")
	if !isLeader {
		becomeLeader()
	}
	isWorking = true
	mutex.Lock()
	tx := transactions[req.TxID]
	mutex.Unlock()
	if !tx.IsAborted && !tx.IsCommitted {
		abort(req.TxID)
	}
	*resp = true
	printState()
	broadcastState()
	isWorking = false
	return nil
}

func (p *KVServer) Commit(req CommitRequest, resp *CommitResponse) error {
	fmt.Println("\n Received a call to Commit")

	if !isLeader {
		becomeLeader()
	}
	isWorking = true
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
		tx.CommitId = nextGlobalCommitId
		mutex.Lock()
		transactions[req.TxID] = tx
		mutex.Unlock()
		*resp = CommitResponse{true, nextGlobalCommitId, ""}
		nextGlobalCommitId++
	}
	printState()
	broadcastState()
	isWorking = false
	return nil
}

func (p *KVServer) Get(req GetRequest, resp *GetResponse) error {
	fmt.Println("\nReceived a call to Get()")
	if !isLeader {
		becomeLeader()
	}
	isWorking = true
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
		for !canAccess {
			var ids []int
			if isDeadlock(req.TxID, trId, &ids) {
				isAbort := resolveDeadLock(req.TxID, ids)
				if isAbort {
					*resp = GetResponse{false, returnVal, "Transaction was aborted"}
					printState()
					broadcastState()
					isWorking = false
					return nil
				}
			} else {
				addToWaitingMap(req.TxID, trId)
				// Allow leadership to be passed
				isWorking = false
			}
			time.Sleep(time.Millisecond * sleepTime)
			// Other transaction aborted me
			mutex.Lock()
			imAborted := transactions[req.TxID].IsAborted
			mutex.Unlock()
			if imAborted {
				*resp = GetResponse{false, returnVal, "Transaction was aborted"}
				printState()
				broadcastState()
				isWorking = false
				return nil
			}
			if !isLeader {
				becomeLeader()
			}
			isWorking = true
			// Reset loop guard
			canAccess, trId = canAccessKey(string(req.Key), req.TxID)
		}
		// Happy path
		removeFromWaitingMap(req.TxID)
		updateKeySet(req.TxID, string(req.Key))
		returnVal = getValue(req.TxID, string(req.Key))
		*resp = GetResponse{true, returnVal, ""}
	}
	printState()
	broadcastState()
	isWorking = false
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
	if ks := transactions[tid].KeySet; ks == nil {
		kset := make(map[string]bool)
		t := transactions[tid]
		t.KeySet = kset
		transactions[tid] = t
	}
	transactions[tid].KeySet[key] = true
	mutex.Unlock()
}

func (p *KVServer) Put(req PutRequest, resp *PutResponse) error {
	fmt.Println("\nReceived a call to Put()")
	if !isLeader {
		becomeLeader()
	}
	isWorking = true
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
		for !canAccess {

			fmt.Println("transactionID:", req.TxID, "Can't Access!! Key:", req.Key, "owned by:", trId)
			var ids []int
			// There is a cycle starting at trId and ending at me
			if isDeadlock(req.TxID, trId, &ids) {
				// aborts correct transaction
				isAbort := resolveDeadLock(req.TxID, ids)
				// I was aborted
				if isAbort {
					*resp = PutResponse{false, "Transaction was aborted"}
					printState()
					broadcastState()
					isWorking = false
					return nil
				}
			} else {
				addToWaitingMap(req.TxID, trId)
				// Allow leadership to be passed
				isWorking = false
			}
			time.Sleep(time.Millisecond * sleepTime)
			// Other transaction aborted me
			mutex.Lock()
			imAborted := transactions[req.TxID].IsAborted
			mutex.Unlock()
			if imAborted {
				*resp = PutResponse{false, "Transaction was aborted"}
				printState()
				broadcastState()
				isWorking = false
				return nil
			}

			if !isLeader {
				becomeLeader()
			}
			isWorking = true
			// Reset loop guard
			canAccess, trId = canAccessKey(string(req.Key), req.TxID)

		}
		// Happy path
		removeFromWaitingMap(req.TxID)
		updateKeySet(req.TxID, string(req.Key))
		setPutTransactionRecord(req)
		*resp = PutResponse{true, ""}
	}
	printState()
	broadcastState()
	isWorking = false
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
	mutex.Unlock()
	tx.IsAborted = true
	tx.PutToDoMap = make(map[string]string)
	tx.KeySet = make(map[string]bool)
	mutex.Lock()
	transactions[txId] = tx
	mutex.Unlock()
	removeFromWaitingMap(txId)
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
		txs := transactions
		mutex.Unlock()

		for k := range txs {
			tr := txs[k]
			_, ok = tr.KeySet[key]
			if ok && !tr.IsAborted && !tr.IsCommitted {
				return false, tr.ID
			}
		}

		// mutex.Unlock()
		// for k := range transactions {
		// 	mutex.Lock()
		// 	tr:= transactions[k]
		// 	mutex.Unlock()
		// 	_, ok = tr.KeySet[key]
		// 	if ok && !tr.IsAborted && !tr.IsCommitted {
		// 		return false, tr.ID
		// 	}
		// }

		return true, 0
	}
}

func setPutTransactionRecord(req PutRequest) {
	mutex.Lock()
	if ptdm := transactions[req.TxID].PutToDoMap; ptdm == nil {
		newptdm := make(map[string]string)
		t := transactions[req.TxID]
		t.PutToDoMap = newptdm
		transactions[req.TxID] = t
	}
	transactions[req.TxID].PutToDoMap[string(req.Key)] = string(req.Value)
	mutex.Unlock()
}

func isKeyInStore(k string) bool {
	_, ok := theValueStore[k]
	return ok
}

func (p *KVServer) NewTransaction(req NewTransactionReq, resp *NewTransactionResp) error {
	fmt.Println("\nReceived a call to NewTransaction()")

	if !isLeader {
		becomeLeader()
	}
	isWorking = true
	tID := nextTransactionId
	nextTransactionId++
	tx := Transaction{tID, req.ClientID, nodeID, make(map[string]string), make(map[string]bool), false, false, 0}
	mutex.Lock()
	transactions[tID] = tx
	mutex.Unlock()
	*resp = NewTransactionResp{tID}
	printState()
	broadcastState()
	isWorking = false
	return nil
}

func becomeLeader() {
	// offset := getOffsetFromLeader()
	isTimeout = false

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	offset := r1.Intn(20)
	fmt.Println("Offset:", offset)

	go startTimer(offset)

	for !isLeader {
		fmt.Println("Waiting to become a leader....")
		if isTimeout {
			fmt.Println("TIMEOUTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT")
			becomeTheLeader()
			break
		}
	}
}

func becomeTheLeader() {
	mutex.Lock()
	nodes := kvnodes
	mutex.Unlock()
	leaderId := getLeaderId()
	leaderNode := nodes[leaderId]
	leaderNode.IsLeader = false
	nodes[leaderId] = leaderNode
	myNode := nodes[nodeID]
	myNode.IsLeader = true
	isLeader = true
	nodes[nodeID] = myNode
	mutex.Lock()
	kvnodes = nodes
	mutex.Unlock()
	broadcastState()
}

func startTimer(offset int) {
	isTimeout = false
	time.Sleep(time.Second*20 + time.Duration(offset))
	isTimeout = true
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

func listenKvNodes() {
	kvServer := rpc.NewServer()
	kv := new(KVServer)
	kvServer.Register(kv)
	l, err := net.Listen("tcp", myKvnodeIpPort)
	checkError("Error in listenKvNodes(), net.Listen()", err, true)
	fmt.Println("Listening for kvnode rpc calls on:", myKvnodeIpPort)
	for {
		conn, err := l.Accept()
		checkError("Error in listenKvNodes(), l.Accept()", err, true)
		go kvServer.ServeConn(conn)
	}
}

// Parses the command line arguments to server.go
func ParseArguments() (err error) {
	arguments := os.Args[1:]
	if len(arguments) == 4 {
		nodesFilePath = arguments[0]
		nodeID, err = strconv.Atoi(arguments[1])
		checkError("Error in ParseArguments(), strconv.Atoi()", err, true)
		myKvnodeIpPort = arguments[2]
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
