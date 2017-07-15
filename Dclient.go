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

	// nodes = []string{"52.233.45.243:2222", "52.175.29.87:2222"}
	// nodes = []string{"52.175.29.87:2222", "52.233.45.243:2222"}
	
	// nodes = []string{"52.233.45.243:2222", "40.69.195.111:2222"}
	// nodes = []string{"40.69.195.111:2222", "52.233.45.243:2222"}

	// nodes = []string{"52.233.45.243:2222", "52.175.29.87:2222", "40.69.195.111:2222"}
	// nodes = []string{"52.175.29.87:2222", "40.6.195.111:2222", "52.233.45.243:2222"}
	nodes = []string{"40.69.195.111:2222", "52.233.45.243:2222", "52.175.29.87:2222"}

	// done := make(chan(int))

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	// success, err := t.Put("Y", "BclientY")
	// fmt.Printf("Put returned: %v, %v\n", success, err)

	success, err := t.Put("Z", "DclientZ")
	fmt.Printf("Put returned: %v, %v\n", success, err)


	// t.Abort()
	// fmt.Println("Just aborted the tx")

	// success, err = t.Put("X", "BclientX")
	// fmt.Printf("Put returned: %v, %v\n", success, err)

	// time.Sleep(time.Second * 15)

	success, err = t.Put("Y", "DclientY")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err := t.Get("Y")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)


	// fmt.Println("Sleeping for 10 seconds...")
	// time.Sleep(time.Second * 10)

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