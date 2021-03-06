package server

import (
  "fmt"
  // "strconv"
  "time"
  // "os"
  "github.com/Shopify/sarama"
  // "git.yelpcorp.com/yelp_meteorite/go/meteorite"
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
    var failedmessages []sarama.ProducerError
    producer := Getkafkaproducer()
    go Collectfailedmessages(producer, &failedmessages)
    // f, err := os.Create("/tmp/flash" + w.topic + strconv.Itoa(w.ID))
    // CheckError(err)
    i := 0
    // dims := make(map[string]string)
    // received_count := meteorite.NewCounter("flash_job_sent", dims)
    go func() {

      for {
        // Check if there are any failed messages and try to resend them
        i = 0
        for i < len(failedmessages) {
            producer.Input() <- failedmessages[i].Msg
            i++
        }
        // Add ourselves into the worker queue.
        w.WorkerQueue <- w.Work
        select {
        case work := <-w.Work:
          // Receive a work request.
          fmt.Printf("worker%d: Received work request for topic: %s and logline: %s\n", w.ID, work.Topic, work.Logline)

          fmt.Printf("Worker %d writing to kafka topic:%s\n", w.ID, w.topic)

          // n3, err := f.WriteString(work.Logline)
          //fmt.Printf("wrote %d bytes\n", n3)
          //CheckError(err)

          producer.Input() <- &sarama.ProducerMessage{
            Topic: w.topic,
            Value: sarama.StringEncoder(work.Logline),
          }
          fmt.Println("Produced a value to kafka")
          // dims["topic"] = work.Topic
          //received_count.Count(1, dims)

        case <-w.QuitChan:
          // We have been asked to stop.
          fmt.Printf("worker%d stopping\n", w.ID)
          return
        }
      }
    }()
}

func Collectfailedmessages(producer sarama.AsyncProducer, messages *[]sarama.ProducerError)  {
    for {

        select {
            case message := <-producer.Errors():
                fmt.Println(message)
                *messages = append(*messages, *message)
        }
    }
}

func Getkafkaproducer() sarama.AsyncProducer {
  config := sarama.NewConfig()
  config.Producer.RequiredAcks = sarama.WaitForAll          // Wait for all in-sync replicas to ack the message
  config.Producer.Compression = sarama.CompressionSnappy    // Compress messages using snappy encoding
  config.Producer.Retry.Max = 3                             // Retry up to 2 times to produce the message
  config.Producer.Flush.Frequency = 500 * time.Millisecond  // Flush every 1 sec
  // config.Producer.Return.Errors = false

  Brokerlist := []string{"kafka:9092"}
  producer, err := sarama.NewAsyncProducer(Brokerlist, config)
  if err != nil {
      fmt.Println("Failed to start Sarama producer:", err)
  }

  return producer
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func(w KafkaWorker) Stop() {
  go func() {
    w.QuitChan <- true
  }()
}
