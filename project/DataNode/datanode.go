package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/sohail-ahmad-1610/code/project/Message"
)

var (
	dataSetLocation string = "/home/sohail/Downloads/crackstation-human-only/DataSet/DataNode_"
)

func main() {

	if len(os.Args) > 3 || len(os.Args) < 3 {
		fmt.Printf("%s\n", "Invalid Command Line Arguments")
		os.Exit(-1)
	}

	myId, er := strconv.Atoi(os.Args[1])
	if er != nil {
		fmt.Println(er)
		os.Exit(-1)
	}
	serverAddress := os.Args[2]

	fmt.Printf("My Id: %d My Server Address: %s\n", myId, serverAddress)

	// Now dial the Given Server Address using go net package........
	conn, err := net.Dial("tcp", serverAddress)

	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Printf("%s %s\n", "Connection Successfully!!!", conn.RemoteAddr())

	dataSetLocation = fmt.Sprintf("%s%d", dataSetLocation, myId)
	fmt.Printf("%s%s\n", "My DataSet Path: ", dataSetLocation)
	msg := message.Message{myId, getFileNames(dataSetLocation), message.REGISTER}

	sendResponseToServer(msg, conn)

	tmp := make([]byte, 1024)
	// Worker wait to get registration confirm by the server........
	conn.Read(tmp)
	serResponse := DecodeServerResponse(tmp)
	//fmt.Printf("" , serResponse)
	if serResponse.Type == message.OK {
		fmt.Printf("%s %d %s\n", "Slave", myId, "Registered to server successfully")
	}

	// Here we open a new port for assigned task in separate go routine.......
	go recvAssignedTask(myId)

	// Construct message to response to the server for heart beat......
	msg = message.Message{myId, "", message.OK}

	for {

		// Slave Read HB message from server......
		conn.Read(tmp)
		serResponse := DecodeServerResponse(tmp)
		if serResponse.Type == message.HB {
			fmt.Printf("Server Response for %s\n", "Heart Beat")
			sendResponseToServer(msg, conn)
		} else if serResponse.Type == message.ABORT {
			os.Exit(1)
		}
		time.Sleep(2 * time.Second)
	}

}
func getFileNames(fileLocation string) string {

	files, err := ioutil.ReadDir(fileLocation)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	msgContent := ""
	separator := "EOFN"
	for _, file := range files {
		//fmt.Println(file.Name())
		msgContent = fmt.Sprintf("%s%s", msgContent, file.Name())
		msgContent = fmt.Sprintf("%s%s", msgContent, separator)
	}
	return msgContent
}

func sendResponseToServer(msg message.Message, conn net.Conn) {

	// Encoding the structure object using GOB go package and Write to server connection........
	bin_buff := new(bytes.Buffer)
	gobobj := gob.NewEncoder(bin_buff)
	gobobj.Encode(msg)
	conn.Write(bin_buff.Bytes())
}

func DecodeServerResponse(tmp []byte) *message.Message {

	// convert bytes into Buffer (which implements io.Reader/io.Writer)
	tmpbuff := bytes.NewBuffer(tmp)

	tmpstruct := new(message.Message)

	// creates a decoder object
	gobobj := gob.NewDecoder(tmpbuff)

	// decodes buffer and unmarshals it into a Message struct
	gobobj.Decode(tmpstruct)

	return tmpstruct

}

func recvAssignedTask(myId int) {

	portNum := fmt.Sprintf(":%d000", (myId + 2))
	ln, err := net.Listen("tcp", portNum)

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	fmt.Println("Slave Start Listening on Separate Port: " + portNum + "\n")
	conn, err := ln.Accept()

	if err != nil {
		log.Println(err)
		os.Exit(1)
		//continue
	}
	go handleAssignedTask(conn)
	//for {	}
}
func handleAssignedTask(conn net.Conn) {

	// Infinite Loop to recive Job and response back to the server.....
	for {

		tmp := make([]byte, 2048)
		// Slave Read START_TASK message from server......
		n, _ := conn.Read(tmp)
		serResponse := DecodeServerResponse(tmp)
		fmt.Println(n)
		//fmt.Sprintf("%d %s %d\n", serResponse.SlaveID, serResponse.Content, serResponse.Type)
		fmt.Printf("Server Assigned the job %s to search\n", serResponse.Content)
		//fmt.Printf("%s %d\n", serResponse.Content[4:], len(serResponse.Content))
		clientQuery := ""
		for _, rune := range serResponse.Content {
			if rune != 00 {
				//fmt.Printf("%d: %c\n", i, rune)
				clientQuery += string(rune)
			}
		}
		fmt.Println(clientQuery)
		if serResponse.Type == message.START_TASK {
			//clientQuery := serResponse.Content
			msg := message.Message{}
			fmt.Printf("Searching Client Query: %s\n", clientQuery)
			if searchPassword(clientQuery, serResponse.SlaveID) {
				msg = message.Message{serResponse.SlaveID, "FOUND", message.OK}
			} else {
				msg = message.Message{serResponse.SlaveID, "NOT FOUND", message.OK}
			}
			fmt.Printf("Sending Searched Client Query: %s Response to Server\n", clientQuery)
			sendResponseToServer(msg, conn)
		}
		//time.Sleep(2 * time.Second)
	}
}

func searchPassword(password string, id int) bool {

	fileName := "File"
	fileName = fmt.Sprintf("%s%d%s", fileName, id, ".txt")
	filePath := dataSetLocation
	filePath = fmt.Sprintf("%s/%s", filePath, fileName)
	fmt.Printf("File Location = %s\n", filePath)
	//fmt.Printf("Need to Search Password = %s\n", password)
	data, err := os.Open(filePath)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer data.Close()
	//file := string(data)  // convert data to string
	scanner := bufio.NewScanner(data)
	//scanner.Split(bufio.ScanLines)
	//password = strings.TrimSpace(password)
	//fmt.Printf("Trimed Password = %s\n", password)
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		if scanner.Text() == password {
			fmt.Printf("Found Password = %s\n", password)
			return true
		}

	}

	return false

}
