package workers

import (
  "fmt"
  "time"
  // "os"

  "thrift_flash/proxies"
)


type Worker struct {
  StatusChan   chan string
  NumRetries   int
  messages     []string
  Filename     string
}

func NewWorker(statuschan chan string, filename string) Worker {
  // Create, and return the worker.
  worker := Worker{
    StatusChan:  statuschan,
    NumRetries:  2,
    messages:    []string{},
    Filename:    filename,
  }
  return worker
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w *Worker) Start(messages []string) {
    w.messages = messages
    go func() {
        fmt.Println("Going to start working on the data")
        fmt.Println(messages)

        sqs_proxy :=  proxies.CreateSqsProxy(
            "kew_devc_sagarp_demo_kew_task_async",
            "/etc/boto_cfg/kew_tools.yaml",
        )
        // sqs_proxy.ListQueues("kew_devc_sagarp_")
        sqs_proxy.WriteMessage("kew_devc_sagarp_")

        for {
            time.Sleep(time.Millisecond * 1000)
            w.StatusChan<-w.Filename
            return
        }
    }()
}

// When we are unable to send data, we will write it to a new file and exit
func (w *Worker) FlushfailedMessagesToDisk(failed_messages []string) {
    fmt.Println("Going to flush data to disk")
}
