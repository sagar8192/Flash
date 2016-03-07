package main

import (
  "fmt"
  "sync"
)

type Queues struct {
    WorkerQueue chan chan WorkRequest
    num_workers int
}

var WorkerQueue chan chan WorkRequest

func Createworker(i int, WorkerQueue chan chan WorkRequest, topic string) {
    fmt.Println("Starting a worker", i)
    worker := NewKafkaWorker(i, WorkerQueue, topic)
    worker.Start()
}

func Createqueue(topic string, queues *[]Queues, m map[string]int, index int) chan chan WorkRequest{
    NewWorkQueue := make(chan chan WorkRequest, 100)
    new_queue := Queues{WorkerQueue: NewWorkQueue, num_workers: 1}
    *queues = append(*queues, new_queue)
    m[topic] = index
    go func () {
        i := 0
        for i < 10 {
            Createworker(i, NewWorkQueue, topic)
            i += 1
        }
    } ()
    return NewWorkQueue
}

func StartDispatcher(wg *sync.WaitGroup) {

  WorkerQueue := make(chan chan WorkRequest, 100)
  num_queues := 0
  var queues []Queues

  go func() {
    for {
      select {
      case work := <-WorkQueue:
        fmt.Println("Received work requeust %s and %s \n", work.Topic, work.Logline)

        // Check if the there is a worker for this particular topic
        // Create one if it doesn't exist
        value, ok := m[work.Topic]
        if ok {
                fmt.Println("value: ", value)
        } else {
                fmt.Println("Creating a new key for topic %s", work.Topic)
                num_queues += 1
                WorkerQueue = Createqueue(work.Topic, &queues, m, num_queues)
        }

        fmt.Println("Number of work channels is ", len(WorkQueue))
        fmt.Println("Number of worker channels is ", len(WorkerQueue))
        go func() {
          worker := <-WorkerQueue
          fmt.Println("Dispatching work request")
          worker <- work
        }()
      }
    }
  }()
  wg.Done()
}
