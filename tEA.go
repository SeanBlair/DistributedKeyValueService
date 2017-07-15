/*

A trivial client to illustrate how the kvservice library can be used
from an application in assignment 6 for UBC CS 416 2016 W2.

Usage:
go run client.go
*/

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./kvservice"

import (
	"fmt"
	// "time"
)
func main() {
	var nodes []string
	// nodes = []string{"52.233.45.243:2222"}
	nodes = []string{"52.233.45.243:2222", "52.175.29.87:2222", "40.69.195.111:2222"}
	// nodes = []string{"52.175.29.87:2222", "40.6.195.111:2222", "52.233.45.243:2222"}
	// nodes = []string{"40.6.195.111:2222", "52.233.45.243:2222", "52.175.29.87:2222"}
	// done := make(chan(int))

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("X", "AclientX")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	// success, err = t.Put("W", "AclientW")
	// fmt.Printf("Put returned: %v, %v\n", success, err)

	// success, err = t.Put("J", "AclientJ")
	// fmt.Printf("Put returned: %v, %v\n", success, err)

	// fmt.Println("Sleeping for 30 seconds...")
	// time.Sleep(time.Second * 30)

	// t.Abort()
	// fmt.Println("Just aborted the tx")

	// success, err = t.Put("Y", "AclientY")
	// fmt.Printf("Put returned: %v, %v\n", success, err)


	// success, err = t.Put("Z", "Aclient")
	// fmt.Printf("Put returned: %v, %v\n", success, err)

	// success, v, err := t.Get("Z")
	// fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)


	// fmt.Println("waiting 10 seconds for system to figure out it aborted....")
	// time.Sleep(time.Second * 1)

	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	// success, err = t.Put("C", "Aclient")
	// fmt.Printf("Put returned: %v, %v\n", success, err)


	

	// fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)
	// fmt.Println("Successfully aborted!!!! :))")

	// success, err = t.Put("goodbye", "oooooooooo")
	// fmt.Printf("Put returned: %v, %v\n", success, err)

	// success, err = t.Put("hello", "lkjsdglkj")
	// fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)


	

	// t.Abort()

	

	

	c.Close()

	// All peers will wait here
	// <-done
}