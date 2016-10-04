package workers

import (
  // "encoding/json"
  "fmt"
  // "strconv"
  "time"
  // "os"

  "github.com/Shopify/sarama"

  //"thrift_flash/message_formats"
)

type KafkaWorkerUnbuffered struct {
  Producer      sarama.AsyncProducer
  Count         int
  QuitChan      chan bool
  Messages      chan []string
  WorkerQueue   chan chan []string
}

// Create and returns a new object of type KafkaWorkerUnbuffered
func CreateKafkaWorkerUnbuffered(workerQueue chan chan []string) KafkaWorkerUnbuffered {
  // Create, and return the worker.
  kafka_worker := KafkaWorkerUnbuffered{
    Producer:      GetUnbufferedkafkaproducer(),
    Count:         0,
    QuitChan:      make(chan bool),
    WorkerQueue:   workerQueue,
    Messages:      make(chan []string),
  }
  return kafka_worker
}

func (w *KafkaWorkerUnbuffered) Start() {
    var failedmessages []sarama.ProducerError
    // var kafka_message message_formats.KafkaMessage

    //Start in-flight monitor for checking status of sent messages
    go w.StartInFlightMonitor(&failedmessages)

    go func() {
        for {
            // Add overselves to the worker queue
            w.WorkerQueue <- w.Messages
            fmt.Println("Worker has added itself back to the queue")
            select {
                case messages:= <- w.Messages:
                    w.Count += len(messages)
                    // Iterate over all the received messages and send them to kafka
                    for _, message := range messages {
                        // Unmarshal the data into KafkaMessage
                        // json.Unmarshal([]byte(message), &kafka_message)

                        w.Producer.Input() <- &sarama.ProducerMessage{
                          //Topic: kafka_message.topic,
                          Topic: "sagarp-testing",
                          //Value: sarama.StringEncoder(kafka_message.logline),
                          Value: sarama.StringEncoder("sagarp testing some stuff"),
                         }
                        fmt.Println("Produced a value to kafka: %v", message)
                    }
            }
        }
    }()
}

func (w *KafkaWorkerUnbuffered) StartInFlightMonitor(failed_messages *[]sarama.ProducerError)  {
    for {
        select {
            case message := <-w.Producer.Errors():
                fmt.Println(message)
                w.Count -= 1
                *failed_messages = append(*failed_messages, *message)

            case <-w.Producer.Successes():
                w.Count -= 1

        }
    }
}


// Initialize config for Async Producer
func GetUnbufferedkafkaproducer() sarama.AsyncProducer {
  config := sarama.NewConfig()
  // config.Producer.RequiredAcks = sarama.WaitForAll          // Wait for all in-sync replicas to ack the message
  config.Producer.RequiredAcks = sarama.WaitForAll          // Wait for all in-sync replicas to ack the message
  config.Producer.Compression = sarama.CompressionSnappy    // Compress messages using snappy encoding
  config.Producer.Retry.Max = 2                             // Retry up to 2 times to produce the message
  config.Producer.Flush.Frequency = 100 * time.Millisecond  // Flush every 500 msec
  // config.Producer.Return.Errors = false

  Brokerlist := []string{"kafka:9092"}
  producer, err := sarama.NewAsyncProducer(Brokerlist, config)
  if err != nil {
      fmt.Println("Failed to start Sarama producer:", err)
  }

  return producer
}

// We flush failed messages back to disk.
/*
func(w *KafkaWorkerUnbuffered) Flush(messages []sarama.ProducerError) {
    file_name := "failed_" + strconv.FormatInt(time.Now().Unix(), 10)

    fd, err := os.Create(file_name)
    if err != nil {
        fmt.Println("Failed to create file ", file_name)
    }

    var pm *sarama.ProducerMessage
    var kafka_message message_formats.KafkaMessage

    // Convert the format back to json
    for _, message := range messages {
        pm = message.Msg
        // fmt.Println("Failed to write a message with %v", pm.Err)

        kafka_message.Topic  = pm.Topic
        kafka_message.Logline = pm.Value

        jsonMessage, err := json.Marshal(kafka_message)
        if err != nil {
            // This will result in message loss
            fmt.Println(err)
        } else {
            fd.WriteString(string(jsonMessage))
        }
    }
    fd = nil
}
*/
