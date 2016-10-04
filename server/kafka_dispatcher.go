package server

import (
    "fmt"
    "sync"
    "time"

    "thrift_flash/workers"
)


type KafkaDispatcher struct {
    kafka_messages  []string
    lock            sync.Mutex
    messages        chan []string
    WorkerQueue     chan chan []string
    WorkerCount     int
}


func CreateNewKafkaDispatcher() *KafkaDispatcher{
    kd := &KafkaDispatcher{
        kafka_messages: []string{},
        messages:       make(chan []string, 1000000),
        WorkerQueue:    make(chan chan []string, 10000),
        WorkerCount:    0,
    }
    return kd
}

// Responsible for keeping track of all the in memory kafka messages
func (kd *KafkaDispatcher) StartKafkaDispatcher() {
    // Start reading messages in
    go kd.read_messages(100)

    go kd.kafka_dispatcher()
}

func (kd *KafkaDispatcher) read_messages(interval int) {
    fmt.Println("Read messages has been started")
    for {
        time.Sleep(time.Millisecond * time.Duration(interval))
        kd.lock.Lock()

        // Forward the messages to messages channel for kafka dispatcher to process it
        if len(kd.kafka_messages) > 0 {
            kd.messages <- kd.kafka_messages
            fmt.Println("Read %v messages from memory buffer and the number of messages %v", len(kd.kafka_messages), len(kd.messages))
            // truncate the messages to 0
            kd.kafka_messages = []string{}
        }
        kd.lock.Unlock()
    }
}

func (kd *KafkaDispatcher) Write_message(message string) {
    kd.lock.Lock()
    defer kd.lock.Unlock()

    kd.kafka_messages = append(kd.kafka_messages, message)
    // fmt.Println("Appended a new message to the message buffer", len(kd.kafka_messages))
}

func (kd *KafkaDispatcher) kafka_dispatcher() {
    for {
        select {
            case messages := <-kd.messages:
                // Dispatch the work to one of the kakfa workers
                fmt.Println("Data received from the kafka dispatcher")

                // Distribute the data amongst workers
                go func() {
                    // Create chunks of messages to be forwarded to kafka
                    if (len(messages) <= 10000) {
                        kd.SendDataToWorker(messages)
                    } else {
                        counter := 1
                        for counter * 10000 > len(messages) {
                            kd.SendDataToWorker(messages[counter-1 : (counter * 10000) + 1])
                            counter = counter + 1
                        }
                        kd.SendDataToWorker(messages[(counter * 10000) + 1 : len(messages)])
                    }

               }()

        }
    }
}

func (kd *KafkaDispatcher) SendDataToWorker(messages []string){
    // Check if there are any workers available. If not, create new workers.
    fmt.Println("Number of workers available is %d", len(kd.WorkerQueue))
    if len(kd.WorkerQueue) == 0 && kd.WorkerCount < 10 {
        // Spawn a new kafka worker
        w := workers.CreateKafkaWorkerUnbuffered(kd.WorkerQueue)
        w.Start()
        fmt.Println("Created a new kafka worker")
        kd.WorkerCount += 1
    }

    worker := <- kd.WorkerQueue
    worker <- messages
    // fmt.Println("Dispatched the messages!!!")
}
