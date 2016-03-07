package server

import (
  "fmt"
  "strconv"
  "os"
)

// NewWorker creates, and returns a new Worker object. Its only argument
// is a channel that the worker can add itself to whenever it is done its
// work.
func NewKafkaWorker(id int, workerQueue chan chan WorkRequest, topic string) KafkaWorker {
  // Create, and return the worker.
  worker := KafkaWorker{
    ID:          id,
    Work:        make(chan WorkRequest),
    WorkerQueue: workerQueue,
    QuitChan:    make(chan bool),
    topic:       topic,
  }
  return worker
}

type KafkaWorker struct {
  ID          int
  Work        chan WorkRequest
  WorkerQueue chan chan WorkRequest
  QuitChan    chan bool
  topic       string
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w KafkaWorker) Start() {
    f, err := os.Create("/tmp/Flash_" + w.topic + strconv.Itoa(w.ID))
    CheckError(err)

    go func() {

      for {
        // Add ourselves into the worker queue.
        w.WorkerQueue <- w.Work
        select {
        case work := <-w.Work:
          // Receive a work request.
          fmt.Printf("worker%d: Received work request for topic: %s and logline: %s\n", w.ID, work.Topic, work.Logline)

          fmt.Printf("Worker %d writing to kafka topic:%s\n", w.ID, w.topic)

          n3, err := f.WriteString(work.Logline)
          fmt.Printf("wrote %d bytes\n", n3)
          CheckError(err)

        case <-w.QuitChan:
          // We have been asked to stop.
          fmt.Printf("worker%d stopping\n", w.ID)
          return
        }
      }
    }()
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func(w KafkaWorker) Stop() {
  go func() {
    w.QuitChan <- true
  }()
}
