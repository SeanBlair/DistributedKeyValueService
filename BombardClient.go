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
	nodes = append(nodes, "localhost:2222")
	done := make(chan(int))

	for i := 0; i < 7; i++ {
		go hitKvsericeSameKey(i)
		// go hitKvsericeDifferentKey(i)
	}

	// All peers will wait here
	<-done

}

func hitKvsericeDifferentKey(i int) {
	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put(kvservice.Key(strconv.Itoa(i)), "Aclient")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, commitId, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, commitId, err)
}

func hitKvsericeSameKey(i int) {
	iStr := strconv.Itoa(i)
	val := "Aclient" + iStr
	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("A", kvservice.Value(val))
	fmt.Printf("Put returned: %v, %v\n", success, err)

	time.Sleep(time.Second)

	success, commitId, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, commitId, err)
}