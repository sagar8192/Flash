package collectors

import (
    "encoding/json"
    "fmt"
    "net"
    "sync"
    //"git.yelpcorp.com/yelp_meteorite/go/meteorite"

    "thrift_flash/message_formats"
)

func CheckError(err error) {
    if err  != nil {
        fmt.Println("Error: " , err)
    }
}

func StartUdpCollector(ws *sync.WaitGroup) {
    defer ws.Done()

    ServerAddr,err := net.ResolveUDPAddr("udp",":10001")
    CheckError(err)

    // Any ip and port 10001
    ServerConn, err1 := net.ListenUDP("udp", ServerAddr)
    CheckError(err1)
    defer ServerConn.Close()

    buf := make([]byte, 2048)
    var request message_formats.WorkRequest

    var requests []message_formats.WorkRequest

    for {
        n, _, _ := ServerConn.ReadFromUDP(buf)
        // ServerConn.ReadFrom(buf)
        fmt.Println("Received a new request....")
        json.Unmarshal(buf[:n], &request)
        requests = append(requests, request)
    }
}
