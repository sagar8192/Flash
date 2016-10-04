package server

import (
  "fmt"
  // "time"
  "os"
  // "git.yelpcorp.com/yelp_meteorite/go/meteorite"

  "thrift_flash/message_formats"
)

type Monitor struct {
  StatusChan        chan string
  Files_to_monitor  chan message_formats.File_count_pair
}

func CreateNewFlightMonitor() Monitor{
    monitor := Monitor{
        StatusChan:        make(chan string, 10000),
        Files_to_monitor:  make(chan message_formats.File_count_pair, 10000),
    }
    return monitor
}

func (m Monitor) Start() {

    go func() {
        pairs := make(map[string]int)

        for {
            select {
                case filename:=<-m.StatusChan:
                    fmt.Println("Received status",filename)

                    // Check if value exists in the dict
                    if value, exists := pairs[filename]; exists {
                        if value == 0 || value == 1{
                            fmt.Println("Removing the rotated file!!!", filename)
                            DeleteFile(filename)
                        } else {
                            pairs[filename] = value - 1
                        }
                    }else{
                        fmt.Println("Status received before getting the file to delete!!!", filename)
                        pairs[filename] = -1
                    }

                case pair:=<-m.Files_to_monitor:
                    fmt.Println("Received a new file to monitor with these many go routines", pair.Filename, pair.Count)
                    if value, exists := pairs[pair.Filename]; exists {
                        if value + pair.Count == 0 {
                            DeleteFile(pair.Filename)
                        }else{
                            pairs[pair.Filename] = pairs[pair.Filename] + pair.Count
                        }
                    }else{
                        pairs[pair.Filename] = pair.Count
                    }

           }
      }
  }()
}

func DeleteFile(filename string){
     fmt.Println("Removing the rotated file!!!", filename)

     err := os.Remove(filename)
     if err != nil {
         fmt.Println("Could not delete file %s exiting...", filename)
     }
}
