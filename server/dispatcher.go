package server

import (
  "bufio"
  "fmt"
  "os"
  "strings"
  "sync"

  "thrift_flash/message_formats"
  "thrift_flash/workers"
)

func Startdispatcher(wg *sync.WaitGroup, fm *RotateWriter) {
    defer wg.Done()

    go func() {
        monitor := CreateNewFlightMonitor()
        monitor.Start()

        payloads := make(map[string][]string)

        for {
            select {
                case file := <-fm.rotated_file_names:
                    fmt.Println("Received a rotated file\n", file)

                    fd, err := os.Open(file)
                    if err != nil {
                        fmt.Println("Could not open file %s exiting...", file)
                    }

                    // Read the file
                    scanner := bufio.NewScanner(fd)
                    for scanner.Scan() {
                        payload := scanner.Text()
                        s := strings.Split(payload, ":")
                        payloads[s[0]] = append(payloads[s[0]], s[1])
                    }
                    fd.Close()
                    fd = nil

                    Dispatch(&payloads, file, monitor)
                    payloads = make(map[string][]string)
            }
        }
    }()
}

func Dispatch(payloads *map[string][]string, file string, monitor Monitor) {
    // Iterate over the payloads map
    for key, value := range *payloads {
        if key == "SQS" {
            // Check the length of value and split the workload accrodingly
            worker := workers.NewSqsWorker(monitor.StatusChan, file, "/etc/boto_cfg/kew_tools.yaml")
            worker.Start()
        }
        fmt.Println(value)
    }
    fmt.Println("Going to start a worker!!!...")
    i := 0
    for i<1 {
        worker := workers.NewSqsWorker(monitor.StatusChan, file, "/etc/boto_cfg/kew_tools.yaml")
        worker.Start()
        i+=1
    }

    fmt.Println("Sending the rotated file to in flight monitor", file)
    monitor.Files_to_monitor<-message_formats.File_count_pair{Filename: file, Count: 2}
}
