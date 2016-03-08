package main

import (
    "encoding/json"
    "fmt"
    "net"
)

type Request struct {
    Topic   string
    Logline string
}

func CheckError(err error) {
    if err  != nil {
        fmt.Println("Error: " , err)
    }
}

func main() {
        ServerAddr,err := net.ResolveUDPAddr("udp","127.0.0.1:10001")
        CheckError(err)

        LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
        CheckError(err)

        Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
        CheckError(err)
        defer Conn.Close()
        i := 0
        for i<1 {
          request := Request{
            "services.kew.tasks",
            "First log line ever",
          }
          jsonRequest, err := json.Marshal(request)
          i++
          buf := []byte(jsonRequest)
          _,err1 := Conn.Write(buf)
          if err != nil {
            fmt.Println(request, err1)
          }
        }
}
