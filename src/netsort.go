package main

import (
	"time"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"bytes"
	"sort"
	"bufio"
	"net"
)

//Convert Into Key Valur Pair
type Number struct {
	key []byte
	value  []byte
}

var Data []Number

var ch = make(chan Number, 1)
var m = make(chan int, 1)


type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func handleConnection(conn net.Conn){
	defer conn.Close()
	fmt.Println("Serever made Connection with ",conn.RemoteAddr().String())
	for {
		buffer := make([]byte, 100)
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Closed connection with ",conn.RemoteAddr().String())
			break
		}
		t := Number{}
		t.key = buffer[:10]
		t.value = buffer[:100]
		ch <- t
	}
	m <- 1
	time.Sleep(5 * time.Second)
}

func listenforData(scs ServerConfigs, serverId int){
	host := ""
	port := ""
	servers := scs.Servers
	for _, sc := range servers {
			if sc.ServerId == serverId {
				host = sc.Host
				port = sc.Port
			}
	}
	fmt.Println("Runnig server on: ", host, port)
	l, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		fmt.Println("Error while listening:", err)
		log.Panic(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil{
			log.Panicln(err)
		}
		go handleConnection(conn)
	}
}


func consolidateData( outputPath string, number int){
	fmt.Println("consolidating the Data")
	temp := 0
	for {
		flag := false
		elem := Number{}
		t:=0
		select {
		case 	elem = <- ch:
			Data = append(Data, elem)
		case t= <- m:
			 temp += t
			 if temp == number {
				 flag = true
				 break
			 }
		 }
		 if flag {
			 break
		 }
	}
	time.Sleep(5 * time.Second)
	fmt.Println(" Len of Data Received   ",len(Data))
	//Sort by key value
	sort.Slice(Data, func(i, j int) bool { return bytes.Compare(Data[i].key, Data[j].key) < 0 })

	// Write in file object
	writef, writeErr := os.Create(outputPath)
	if writeErr != nil {
			log.Fatal(writeErr)
	}
	defer writef.Close()
	w := bufio.NewWriter(writef)
	for _, t := range Data {
		_, err :=	w.Write(t.value)
		if err != nil {
			fmt.Println("Error writing file:", err)
			log.Fatal(err)
		}
	}
	w.Flush()
}

func sendData(scs ServerConfigs, inputFilePath string){
	//Read the file object
	fmt.Println("Sending Data...")
	fileData, readErr:=ioutil.ReadFile(inputFilePath)
	if readErr != nil {
		fmt.Println("Error reading file:", readErr)
	 	log.Fatal(readErr)
	}
	var data []Number
	for i := 0;i < len(fileData); i += 100 {
		t := Number{}
		t.key = fileData[i:i+10]
		t.value = fileData[i:i+100]
		data=append(data,t)
	}
	conn := make(map[int]net.Conn)
	servers := scs.Servers
	var err error
	for _, sc := range servers {
		conn[sc.ServerId], err = net.Dial("tcp", sc.Host+":"+sc.Port)
		if err != nil {
			fmt.Println("Error in Dial:", err)
			log.Panicln(err)
		}
		defer conn[sc.ServerId].Close()
	}

	for _, t := range data {
		firstByte := t.key[0]
		fb := firstByte & 192
		fb = fb >> 6
		_, err := conn[int(fb)].Write(t.value)
		if err != nil {
			fmt.Println("Error in sending data:", err)
		}
	}
	fmt.Println(" Len of Data sent ",len(data))
	time.Sleep(5 * time.Second)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs.Servers)


	go listenforData(scs, serverId)
	time.Sleep(15 * time.Second)
	go sendData(scs, os.Args[2])
	consolidateData(os.Args[3], len(scs.Servers))
}
