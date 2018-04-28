package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sohail-ahmad-1610/code/project/Message"
)

// Slave is a 'struct' used for manage slave list.....
type Slave struct {
	slaveMessage  *message.Message
	slaveConn     net.Conn
	slaveStatus   int
	isJobComplete bool
}

var (
	clientPassword     string
	clientResponse     string
	isClientQueryFound bool
	isSearchComplete   bool
	slavesSlice        []Slave
)

func main() {

	// Decalre variables for ports and set default values if command line arguments are missing....

	clientPortNum := 2020
	workerPortNum := 5050

	// Condition to check if commandline arguments Given then use commandline arguments.......
	if len(os.Args) == 3 {

		portNum, err := strconv.Atoi(os.Args[1])

		if err != nil {
			fmt.Printf("%s %s\n", "Error in converting client port number ", err)
			os.Exit(-1)
		}
		clientPortNum = portNum
		portNum, err = strconv.Atoi(os.Args[2])

		if err != nil {
			fmt.Printf("%s %s\n", "Error in converting worker port number ", err)
			os.Exit(-1)
		}
		workerPortNum = portNum
	}

	fmt.Printf("Client Port: %d  Worker Port: %d\n", clientPortNum, workerPortNum)

	//Make A separate Go rotine to handle every new client request on Server........
	go handleClients(clientPortNum)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", workerPortNum))
	if err != nil {
		fmt.Printf("Error in listening on worker port %s\n", err)
		os.Exit(-1)
	}

	// Inifinite loop for multiple worker connection Accept and multi-threads for worker request.......
	for {

		fmt.Printf("%s %d\n", "Server is Listening for Worker request on port: ", workerPortNum)
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("%s %s\n", "Error in accepting worker request and ignore the request ", err)
			continue
		}

		//Make A separate Go rotine to handle every new worker Connection on Server........
		go handleWorkersConnection(conn)
	}

}

// It is a Go routine function to handle new client Connection......
func handleClients(cPortNum int) {

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cPortNum))
	if err != nil {
		fmt.Printf("%s %s\n", "Error in listening on client port", err)
		os.Exit(-1)
	}

	// Inifinite loop for multiple clients connection and multi-threads for client request.......
	for {

		fmt.Printf("%s %d\n", "Server is Listening for clients request on port: ", cPortNum)
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Error in accepting client request and ignore the request %s\n", err)
			continue
		}

		//Make A separate Go rotine to handle every new client Connection on Server........
		go handleClientsConnection(conn)

	}

}

// A Go routine function to communicate with clients......
func handleClientsConnection(conn net.Conn) {

	clientData := make([]byte, 1024)

	// Receive Initial message from Client......
	conn.Read(clientData)

	clientPassword = string(clientData)

	fmt.Printf("New Client Request %s Received Successfully!!!\n", clientPassword)
	// Infinite loop to check either search process complete or not.......
	for !isSearchComplete {

	}
	fmt.Printf("Sending Response %s to Client\n", clientResponse)
	// Here we need to send response to the client request.......
	conn.Write([]byte(clientResponse))

	// Sending Abort message to all clients.....
	fmt.Printf("Sending Abort Mesage to Worker\n")
	sendAbortMessage()
	fmt.Printf("All Workers Abort Now\n")
	fmt.Printf("Client Query Successfully Executed and Query Response Sent to the Client\n")
}

// A Go routine function to communicate with workers......
func handleWorkersConnection(conn net.Conn) {

	// create a temp buffer
	tmp := make([]byte, 1024)

	// Read fileNames each worker have.......
	conn.Read(tmp)
	msg := DecodeWorkerResponse(tmp)

	// lets print out!
	//fmt.Println(msg) // reflects.TypeOf(tmpstruct) == Message{}

	if msg.Type == message.REGISTER {

		msgResponse := message.Message{0, "", message.OK}
		sendResponseToSlave(msgResponse, conn)
		time.Sleep(1 * time.Second)
		// Making a new Slave and Save in slavesSlice..........
		newSlave := Slave{msg, conn, message.ALIVE, false}
		ind := isSlaveExist(newSlave.slaveMessage.SlaveID)

		if ind != -1 {
			// An Existing Slave just update the slave status.....
			slavesSlice[ind].slaveStatus = message.ALIVE
			slavesSlice[ind].slaveConn = newSlave.slaveConn
			slavesSlice[ind].isJobComplete = newSlave.isJobComplete
			slavesSlice[ind].slaveMessage = newSlave.slaveMessage
			fmt.Println("An Existing Slave in Slave Slice is Update")
		} else {
			fmt.Println("Append New Slave in Slave Slice")
			slavesSlice = append(slavesSlice, newSlave)
		}

		// A separate Go Routine to send HeartBeat to the connected slaves and kill if
		// Slave is not responding........
		go sendHeartBeat(newSlave)

		if !isClientQueryFound {
			go AssignTaskToSlave(newSlave)
		}
	}

	// We can wait here to connect other clients and then assign task to them.........

	// for _, slave := range slavesSlice {
	//
	// 	if !isClientQueryFound {
	// 		go AssignTaskToSlave(slave)
	// 	} else {
	// 			isSearchComplete = true
	// 			break
	// 	}
	//
	// }
	// }else {
	// 		isSearchComplete = true
	// }

}
func sendResponseToSlave(msg message.Message, conn net.Conn) {

	// Encoding the structure object using GOB go package and Write to server connection........
	bin_buff := new(bytes.Buffer)
	gobobj := gob.NewEncoder(bin_buff)
	gobobj.Encode(msg)
	conn.Write(bin_buff.Bytes())
}

func DecodeWorkerResponse(tmp []byte) *message.Message {

	// convert bytes into Buffer (which implements io.Reader/io.Writer)
	tmpbuff := bytes.NewBuffer(tmp)

	tmpstruct := new(message.Message)

	// creates a decoder object
	gobobj := gob.NewDecoder(tmpbuff)

	// decodes buffer and unmarshals it into a Message struct
	gobobj.Decode(tmpstruct)

	return tmpstruct

}
func sendAbortMessage() {

	for _, slave := range slavesSlice {
		if slave.slaveStatus != message.KILL {
			msg := message.Message{-1, "", message.ABORT}
			sendResponseToSlave(msg, slave.slaveConn)
		}
	}
}
func sendHeartBeat(newSlave Slave) {

	timeoutDuration := 10 * time.Second

	for {

		time.Sleep(10 * time.Second)
		// Set a deadline for reading. Read operation will fail if no data
		// is received after deadline.
		newSlave.slaveConn.SetReadDeadline(time.Now().Add(timeoutDuration))
		msgResponse := message.Message{-1, "", message.HB}
		fmt.Printf("Sending Heart Beat Message to Slave %d\n", newSlave.slaveMessage.SlaveID)
		sendResponseToSlave(msgResponse, newSlave.slaveConn)
		// create a temp buffer
		tmp := make([]byte, 1024)

		// Read fileNames each worker have.......
		_, er := newSlave.slaveConn.Read(tmp)
		if er != nil {
			fmt.Printf("Failed to Receive Heart Beat Response from Slave %d and has been killed\n", newSlave.slaveMessage.SlaveID)
			break
		}

		// msg := DecodeWorkerResponse(tmp)
		//
		// // lets print out!
		// fmt.Println(msg) // reflects.TypeOf(tmpstruct) == Message{}
	}
	newSlave.slaveStatus = message.KILL
	newSlave.slaveConn.Close()

}

// AssignTaskToSlave is a function used to send client query to slave
// As well as schedule all the slave replica files.......
func AssignTaskToSlave(slave Slave) {

	if slave.slaveStatus == message.ALIVE || slave.slaveStatus == message.IDOL {

		workerAddress := "127.0.0.2"
		workerPort := fmt.Sprintf(":%d000", (slave.slaveMessage.SlaveID + 2))
		workerAddress = fmt.Sprintf("%s%s", workerAddress, workerPort)

		// Now dial the Worker Address using go net package........
		conn, err := net.Dial("tcp", workerAddress)

		if err != nil {
			// handle error
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Printf("%s %s\n", "Connection Successfully to worker new port!!!", conn.RemoteAddr())

		if !isAssignedJobComplete(slave, conn, 0) {

			slave.isJobComplete = true
			if slave.slaveStatus != message.KILL {

				// Getting Replica files of this slave and schedule on it..............
				separator := "EOFN"
				var slaveFileNamesSlice []string = strings.Split(slave.slaveMessage.Content, separator)
				myFileName := "File"
				myFileName = fmt.Sprintf("%s%d%s", myFileName, slave.slaveMessage.SlaveID, ".txt")
				var replicaFilesName []string

				for _, fName := range slaveFileNamesSlice {
					if strings.EqualFold(fName, myFileName) {
						continue
					}
					replicaFilesName = append(replicaFilesName, fName)
					//fmt.Println(fName)
				}

				// Now iterate over replica files and schedule on this slave one by one.
				// if that file slave/workhorse has been killed
				replicaSlaveId := 0
				for _, fName := range replicaFilesName {

					if !strings.EqualFold(fName, "") {
						ind := strings.LastIndex(fName, ".")
						//fmt.Println(ind)
						sId := fName[ind-1 : ind]
						slaveId, err := strconv.Atoi(sId)
						replicaSlaveId = slaveId

						if err != nil {
							// handle error
							fmt.Println(err)
							os.Exit(1)
						}

						fmt.Printf("%s%d\n", "Extracted Slave ID: ", slaveId)
						slaveInd := isSlaveExist(slaveId)
						if slaveInd != -1 {
							// It means slave exist but to check either alive or not.....
							existingSlave := slavesSlice[slaveInd]
							if existingSlave.slaveStatus == message.KILL && !existingSlave.isJobComplete {
								// Do that slave job also.....
								isAssignedJobComplete(slave, conn, replicaSlaveId)
							}
						} else {
							// That Slave not arrive yet........
							isAssignedJobComplete(slave, conn, replicaSlaveId)
						}
					} // End of if block that check either fileNames empty or not......
				} // End of for loop of replica fileNames....
			}
		} // End of if block....
	}
}
func isSlaveExist(slaveID int) int {

	for i, oldSlave := range slavesSlice {
		if slaveID == oldSlave.slaveMessage.SlaveID && oldSlave.slaveStatus != message.KILL {
			return i
		}
	}
	return -1
}
func isAssignedJobComplete(slave Slave, conn net.Conn, repSlaveId int) bool {

	// Declare msg variable to construct client query.....
	msg := message.Message{}

	// Check either Replica Job or Original one.....
	if repSlaveId != 0 {
		msg = message.Message{repSlaveId, clientPassword, message.START_TASK}
	} else {
		msg = message.Message{slave.slaveMessage.SlaveID, clientPassword, message.START_TASK}
	}

	// Sending Client request to worker......
	fmt.Printf("Client Query= %s\n%s%d\n", clientPassword, msg.Content, msg.Type)
	sendResponseToSlave(msg, conn)
	slave.slaveStatus = message.BUSY

	// Receive Worker response client query found or not found......
	tmp := make([]byte, 1024)
	// Read worker response here.......
	fmt.Printf("Waiting for Slave %d Query Response\n", slave.slaveMessage.SlaveID)
	_, er := conn.Read(tmp)
	if er != nil {
		fmt.Println(er)
		os.Exit(1)
	}
	slaveQueryRes := DecodeWorkerResponse(tmp)

	// lets print out!
	fmt.Printf("%s %d %s %s\n", "Worker ", slave.slaveMessage.SlaveID, "Response: ", slaveQueryRes.Content) // reflects.TypeOf(tmpstruct) == Message{}
	slave.slaveStatus = message.IDOL
	//isMoreTask := false
	fmt.Printf("%s %d\n", slaveQueryRes.Content, len(slaveQueryRes.Content))
	queryResponse := ""
	for _, rune := range slaveQueryRes.Content {
		if rune != 00 {
			//fmt.Printf("%d: %c\n", i, rune)
			queryResponse += string(rune)
		}
	}
	fmt.Println(queryResponse)
	if strings.EqualFold(queryResponse, "FOUND") {
		isClientQueryFound = true
		isSearchComplete = true
		clientResponse = queryResponse
		return true
	}
	return false
}
