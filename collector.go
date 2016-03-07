package main

import (
    "encoding/json"
    "fmt"
    "net"
    "os"
    "sync"
)

/* A Simple function to verify error */
func CheckError(err error) {
    if err  != nil {
        fmt.Println("Error: " , err)
        os.Exit(0)
    }
}

var WorkQueue = make(chan WorkRequest, 100)

func Collector(ws *sync.WaitGroup) {
    /* Lets prepare a address at any address at port 10001*/
    ServerAddr,err := net.ResolveUDPAddr("udp",":10001")
    CheckError(err)

    /* Now listen at selected port */
    ServerConn, err := net.ListenUDP("udp", ServerAddr)
    CheckError(err)
    defer ServerConn.Close()

    buf := make([]byte, 2048)
    // buf := make([]byte, unsafe.Sizeof(Message))
    var request WorkRequest

    for {
        n,addr,err := ServerConn.ReadFromUDP(buf)
        err1 := json.Unmarshal(buf[:n], &request)
        fmt.Println("Received from ", addr)

        if err != nil {
            fmt.Println("Error: ",err1)
        }
        if err1 != nil {
            fmt.Println("Problem in unmarchalling data: ",err1)
        }

        // Push the work onto the queue.
        WorkQueue <- WorkRequest{Topic: request.Topic, Logline: request.Logline}
        fmt.Println("Work request queued", request.Topic, request.Logline)
    }
    ws.Done()
}
