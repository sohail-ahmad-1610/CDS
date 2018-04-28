package main

import (
	"fmt"
	"net"
	"os"
)

func main() {

	//fmt.Printf("%s %d\n", "Client Program running ", len(os.Args))

	// Condition to check invalid command line arguments........
	if len(os.Args) > 3 || len(os.Args) < 2 {

		fmt.Printf("Command line arguments error. There must be two arguments including password text and server address\n")
		os.Exit(-1)

	}

	// receiving commandline argument as string........
	searchText := os.Args[1]
	serverAddress := os.Args[2]

	fmt.Printf("Given Text: %s Givent Server Address: %s\n", searchText[12:], serverAddress[8:])

	// Now dial the Given Server Address using go net package........
	conn, err := net.Dial("tcp", serverAddress[8:])

	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Printf("%s %s\n", "Connection Successfully!!!", conn.RemoteAddr())

	// Make a slice buffer to receive the Server response......
	serverResponseBuffer := make([]byte, 10)

	fmt.Printf("%s\n", "Given password query sent to the server")
	conn.Write([]byte(searchText[12:]))

	fmt.Printf("%s\n", "Please Wait for the server response")

	// Client block here and unblock when server Response...........
	conn.Read(serverResponseBuffer)

	// Print the server response.....
	fmt.Printf("Server Response is: %s\n", string(serverResponseBuffer))

}
