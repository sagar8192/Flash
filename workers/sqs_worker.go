package workers

import (
  "fmt"
  "time"
  // "os"

  "thrift_flash/config"
)


type SqsWorker struct {
  Worker
  AwsCredsFile   string
}

// Create and returns a new object of type SqsWorker
func NewSqsWorker(statuschan chan string, filename string, awscredsfile string) SqsWorker {
  // Create, and return the worker.
  worker := Worker{
    StatusChan:  statuschan,
    NumRetries:  2,
    messages:    []string{},
    Filename:    filename,
  }
  sqs_worker := SqsWorker{
    Worker:        worker,
    AwsCredsFile:  awscredsfile,
  }
  return sqs_worker
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w SqsWorker) Start() {

    aws := config.Create_config(w.AwsCredsFile)
    aws.Read_aws_keys()

    fmt.Println("Private key is", aws.Keys.Access_key_id)
    fmt.Println("Public key is", aws.Keys.Secret_access_key)

    go func() {
        fmt.Println("Going to start working on the data")
        for {
            time.Sleep(time.Millisecond * 1000)
            w.StatusChan<-w.Filename
            return
        }
    }()
}
