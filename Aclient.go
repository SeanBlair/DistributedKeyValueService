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
	"time"
)
func main() {
	var nodes []string
	nodes = append(nodes, "65.52.189.58:2222")
	// nodes = append(nodes, "bob:2010")

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("A", "Aclient")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, err = t.Put("Z", "Aclient")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err := t.Get("Z")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)



	// success, err = t.Put("C", "Aclient")
	// fmt.Printf("Put returned: %v, %v\n", success, err)


	

	// fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)
	// fmt.Println("Successfully aborted!!!! :))")

	success, err = t.Put("goodbye", "oooooooooo")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, err = t.Put("hello", "lkjsdglkj")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)


	time.Sleep(time.Second * 10)

	// t.Abort()


	success, txID, err := t.Commit()
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()
}