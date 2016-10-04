package collectors

import (
    "fmt"
    "net"
    "net/rpc"
    "sync"

    "github.com/samuel/go-thrift/examples/scribe"
    "github.com/samuel/go-thrift/thrift"

    "thrift_flash/server"
)

type scribeServiceImplementation int
var rotator *server.RotateWriter
var kafka_dispatcher *server.KafkaDispatcher

func (s *scribeServiceImplementation) Log(messages []*scribe.LogEntry) (scribe.ResultCode, error) {
    for _, m := range messages {
        // fmt.Printf("CATEGORY: %+V MSG: %+v\n", m.Category, m.Message)
        // rotator.Write([]byte(m.Message))
        /*
        if m.Category == "kafka" {
            // fmt.Println("A new kafka message received")
            kafka_dispatcher.Write_message(m.Message)
        } else {
            // fmt.Println("A new other than kafka message received")
            rotator.Writestring(m.Category + ":" + m.Message)
        }
        */
        kafka_dispatcher.Write_message(m.Message)
    }
    return scribe.ResultCodeOk, nil
}

func Scribeserver(ws *sync.WaitGroup, fm *server.RotateWriter) {
    rotator = fm

    kd := server.CreateNewKafkaDispatcher()
    kafka_dispatcher = kd
    kafka_dispatcher.StartKafkaDispatcher()

    defer ws.Done()
    scribeService := new(scribeServiceImplementation)
    rpc.RegisterName("Thrift", &scribe.ScribeServer{Implementation: scribeService})

    ln, err := net.Listen("tcp", ":10002")
    if err != nil {
        panic(err)
    }

    for {
        conn, err := ln.Accept()
        if err != nil {
            fmt.Printf("ERROR: %+v\n", err)
            continue
        }

        fmt.Printf("New connection %+v\n", conn)
        t := thrift.NewTransport(thrift.NewFramedReadWriteCloser(conn, 0), thrift.BinaryProtocol)
        go rpc.ServeCodec(thrift.NewServerCodec(t))
    }
}
