package workers

import (
  "encoding/json"
  "fmt"
  // "strconv"
  "time"
  // "os"

  "github.com/Shopify/sarama"

  "thrift_flash/message_formats"
)

type KafkaWorker struct {
  Worker
  Producer  sarama.AsyncProducer
  Count     int
  QuitChan  chan bool
}

// Create and returns a new object of type KafkaWorker
func CreateKafkaWorker(statuschan chan string, filename string) KafkaWorker {
  // Create, and return the worker.
  worker := Worker{
    StatusChan:  statuschan,
    NumRetries:  2,
    messages:    []string{},
    Filename:    filename,
  }
  kafka_worker := KafkaWorker{
    Worker:        worker,
    Producer:      Getkafkaproducer(),
    Count:         0,
    QuitChan:      make(chan bool),
  }
  return kafka_worker
}

func (w *KafkaWorker) Start(messages []string) {
    w.messages = messages
    w.Count = len(messages)
    var failedmessages []sarama.ProducerError
    var kafka_message message_formats.KafkaMessage

    //Start in-flight monitor for checking status of sent messages
    go w.StartInFlightMonitor(&failedmessages)

    go func() {
        for _, message := range w.messages {
            // Unmarshal the data into KafkaMessage
            json.Unmarshal([]byte(message), &kafka_message)

            // w.Producer.Input() <- &sarama.ProducerMessage{
            //  Topic: kafka_message.topic,
            //  Value: sarama.StringEncoder(kafka_message.logline),
            // }
            // fmt.Println("Produced a value to kafka")
        }

        // We wait for all the messages to be acknowledge
        select {
            case <-w.QuitChan:
                if len(failedmessages) > 0 {
                    // w.Flush(failedmessages)
                }

                w.StatusChan<-w.Filename
                return
        }
    }()
}

func (w *KafkaWorker) StartInFlightMonitor(failed_messages *[]sarama.ProducerError)  {
    for {
        select {
            case message := <-w.Producer.Errors():
                fmt.Println(message)

                w.Count -= 1
                if w.Count == 0 {
                    w.QuitChan <- true
                }
                *failed_messages = append(*failed_messages, *message)
                break

            case <-w.Producer.Successes():
                w.Count -= 1

                if w.Count == 0 {
                    w.QuitChan <- true
                    break
                }


        }
    }
}


// Initialize config for Async Producer
func Getkafkaproducer() sarama.AsyncProducer {
  config := sarama.NewConfig()
  config.Producer.RequiredAcks = sarama.WaitForAll          // Wait for all in-sync replicas to ack the message
  config.Producer.Compression = sarama.CompressionSnappy    // Compress messages using snappy encoding
  config.Producer.Retry.Max = 2                             // Retry up to 2 times to produce the message
  config.Producer.Flush.Frequency = 5 * time.Millisecond  // Flush every 500 msec
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
func(w *KafkaWorker) Flush(messages []sarama.ProducerError) {
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
