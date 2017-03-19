/*

A trivial client to illustrate how the kvservice library can be used
from an application in assignment 6 for UBC CS 416 2016 W2.

Usage:
go run client.go
*/

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
// import "./kvservice"

import (
	"fmt"
	"./kvservice"
	"time"
	"strconv"
)

var (
	nodes []string
)
func main() {
	nodes = append(nodes, "localhost:4444")
	done := make(chan(int))

	for i := 1; i < 101; i++ {
		// go hitKvsericeSameKey(i)
		// go hitKvsericeDifferentKey(i)
		go hitKvsericeNewTransaction(i)
		// go hitKvsericeNewTransactionAbort(i)
		// go hitKvsericeNewTransactionCommit(i)
	}

	// All peers will wait here
	<-done
}

func hitKvsericeNewTransactionCommit(i int) {
	c := kvservice.NewConnection(nodes)
	fmt.Println("iteration:", i, "NewConnection returned:", c)

	t, err := c.NewTX()
	fmt.Println("iteration:", i, "NewTX returned:", t, err)

	success, txID, err := t.Commit()
	fmt.Println("iteration:", i, "Commit returned:", success, txID, err)
}
 
func hitKvsericeNewTransactionAbort(i int) {
	c := kvservice.NewConnection(nodes)
	fmt.Println("iteration:", i, "NewConnection returned:", c)

	t, err := c.NewTX()
	fmt.Println("iteration:", i, "NewTX returned:", t, err)

	t.Abort()
	fmt.Println("iteration:", i, "successfully aborted... :)")
}

func hitKvsericeNewTransaction(i int) {
	c := kvservice.NewConnection(nodes)
	fmt.Println("iteration:", i, "NewConnection returned:", c)

	t, err := c.NewTX()
	fmt.Println("iteration:", i, "NewTX returned:", t, err)
}


func hitKvsericeDifferentKey(i int) {
	iStr := strconv.Itoa(i)
	val := "Aclient" + iStr
	c := kvservice.NewConnection(nodes)
	fmt.Println("iteration:", i, "NewConnection returned:", c)

	t, err := c.NewTX()
	fmt.Println("iteration:", i, "NewTX returned:", t, err)

	success, err := t.Put(kvservice.Key(strconv.Itoa(i)), kvservice.Value(val))
	fmt.Println("iteration:", i, "Put returned:", success, err)

	success, v, err := t.Get(kvservice.Key(strconv.Itoa(i)))
	fmt.Println("iteration:", i, "Get returned:", success, v, err)

	// t.Abort()
	// fmt.Println("iteration:", i, "successfully aborted... :)")

	success, commitId, err := t.Commit()
	fmt.Println("iteration:", i, "Commit returned:", success, commitId, err)
}

func hitKvsericeSameKey(i int) {
	iStr := strconv.Itoa(i)
	val := "Aclient" + iStr
	c := kvservice.NewConnection(nodes)
	fmt.Println("iteration:", i, "NewConnection returned:", c)

	t, err := c.NewTX()
	fmt.Println("iteration:", i, "NewTX returned:", t, err)

	success, err := t.Put("A", kvservice.Value(val))
	fmt.Println("iteration:", i, "Put returned:", success, err)

	time.Sleep(time.Second)

	success, commitId, err := t.Commit()
	fmt.Println("iteration:", i, "Commit returned:", success, commitId, err)
}