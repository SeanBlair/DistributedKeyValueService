/*

An example stub implementation of the kvservice interface for use by a
client to access the key-value service in assignment 6 for UBC CS 416
2016 W2.

*/

package kvservice

import (
	"fmt"
	"net/rpc"
	"log"
	"os"
	"errors"
	"strings"
	"strconv"
	"net"
	)

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Close the connection.
	Close()
}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is nil, and err is non-nil. If success is
	// false, then all future calls on this transaction must
	// immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit() (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

///////////// Our variables////////////

var (
	kvNodesIpPorts []string
)

type KVServer int

type NewConnectionResp struct {
	IsAlivePort int
}

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
	// Err error 
	Err string
}


type GetRequest struct {
	TxID int
	Key Key
}

type GetResponse struct {
	Success bool
	Value Value 
	// Err error 
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

///////////////////////////////////////




//////////////////////////////////////////////

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	// fmt.Printf("NewConnection with nodes:\n", nodes)
	fmt.Println("Received call to NewConnection() with nodes:", nodes)
	kvNodesIpPorts = nodes

	req := true
	var resp NewConnectionResp
	client, err := rpc.Dial("tcp", kvNodesIpPorts[0])
	checkError("rpc.Dial in getNewConnection()", err, true)
	err = client.Call("KVServer.NewConnection", req, &resp)
	checkError("client.Call(KVServer.NewConnection) in NewConnection(): ", err, true)
	err = client.Close()
	checkError("client.Close() in NewConnection(): ", err, true)

	kvNodeIpPort := kvNodesIpPorts[0]
	kvNodeIp := kvNodeIpPort[:strings.Index(kvNodeIpPort, ":")]
	isAliveConnectionIpPort := kvNodeIp + ":" + strconv.Itoa(resp.IsAlivePort)
	startIsAliveConnection(isAliveConnectionIpPort)
	fmt.Println("IsAlive connection started with kvnode:", kvNodesIpPorts[0], "on ipPort:", isAliveConnectionIpPort)
	c := new(myconn)
	return c
}

func startIsAliveConnection(ipPort string) {
	conn, err := net.Dial("tcp", ipPort)
	checkError("Error in startIsAliveConnection(), net.Dial()", err, true)
	fmt.Println("successfully started an isAlive connection with:", ipPort)
	for {
		buffer := make([]byte, 10)
		n, err := conn.Read(buffer)
		checkError("Error in startIsAliveConnection, conn.Read()", err, true)
		fmt.Println("Received message:", string(buffer[0:n]), "from kvnode:", ipPort)
		fmt.Fprintf(conn, "Pong")
	}
}


//////////////////////////////////////////////
// Connection interface

// Concrete implementation of a connection interface.
type myconn struct {
	// TODO
}

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	fmt.Printf("NewTX\n")
	m := new(mytx)
	m.ID = getNewTXID()
	return m, nil
}

func getNewTXID() int {
	req := true
	var resp NewTransactionResp
	client, err := rpc.Dial("tcp", kvNodesIpPorts[0])
	checkError("rpc.Dial in getNewTXID()", err, true)
	err = client.Call("KVServer.NewTransaction", req, &resp)
	checkError("client.Call(KVServer.NewTransaction) in getNewTXID(): ", err, true)
	err = client.Close()
	checkError("client.Close() in getNewTXID(): ", err, true)
	return resp.TxID
}

// Close the connection.
func (conn *myconn) Close() {
	fmt.Printf("Close\n")
	// TODO
}

// /Connection interface
//////////////////////////////////////////////

//////////////////////////////////////////////
// Transaction interface

// Concrete implementation of a tx interface.
type mytx struct {
	// TODO
	ID int
}

// Retrieves a value v associated with a key k.
func (t *mytx) Get(k Key) (success bool, v Value, err error) {
	fmt.Printf("Get\n")
	req := GetRequest{t.ID, k}
	var resp GetResponse
	client, err := rpc.Dial("tcp", kvNodesIpPorts[0])
	checkError("rpc.Dial in Get()", err, true)
	err = client.Call("KVServer.Get", req, &resp)
	checkError("client.Call(KVServer.Get) Get(): ", err, true)
	err = client.Close()
	checkError("client.Close() in Get(): ", err, true)
	// err = errors.New(resp.Err)
	return resp.Success, resp.Value, errors.New(resp.Err) 
}

// Associates a value v with a key k.
func (t *mytx) Put(k Key, v Value) (success bool, err error) {
	fmt.Printf("Put\n")
	// TODO
	success, err = callPutRPC(t.ID, k, v)
	return
}

func callPutRPC(transactionId int, key Key, value Value) (bool, error) {
	req := PutRequest{transactionId, key, value}
	var resp PutResponse
	client, err := rpc.Dial("tcp", kvNodesIpPorts[0])
	checkError("rpc.Dial in callPutRPC()", err, true)
	err = client.Call("KVServer.Put", req, &resp)
	checkError("client.Call(KVServer.Put) in callPutRPC(): ", err, true)
	err = client.Close()
	checkError("client.Close() in callPutRPC(): ", err, true)
	return resp.Success, errors.New(resp.Err)
}

// Commits the transaction.
func (t *mytx) Commit() (success bool, txID int, err error) {
	fmt.Printf("Commit\n")
	req := CommitRequest{t.ID}
	var resp CommitResponse
	client, err := rpc.Dial("tcp", kvNodesIpPorts[0])
	checkError("rpc.Dial in Commit()", err, true)
	err = client.Call("KVServer.Commit", req, &resp)
	checkError("client.Call(KVServer.Commit) Commit(): ", err, true)
	err = client.Close()
	checkError("client.Close() in Commit(): ", err, true)
	return resp.Success, resp.CommitId, errors.New(resp.Err)
}

// Aborts the transaction.
func (t *mytx) Abort() {
	fmt.Printf("Abort\n")
	req := AbortRequest{t.ID}
	var resp bool
	client, err := rpc.Dial("tcp", kvNodesIpPorts[0])
	checkError("rpc.Dial in Abort()", err, true)
	err = client.Call("KVServer.Abort", req, &resp)
	checkError("client.Call(KVServer.Abort) Abort(): ", err, true)
	err = client.Close()
	checkError("client.Close() in Abort(): ", err, true)
}

// /Transaction interface
//////////////////////////////////////////////


// Prints msg + err to console and exits program if exit == true
func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
