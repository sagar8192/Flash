package main

import (
  "fmt"
  "sync"
)

func Startcollector() {

  var wg sync.WaitGroup

  // Start the dispatcher.
  fmt.Println("Starting the dispatcher")
  wg.Add(1)
  go StartDispatcher(&wg)

  fmt.Println("Starting collector")
  wg.Add(1)
  go Collector(&wg)

  // Collector is up and running!
  fmt.Println("UDP server has been started")
  wg.Wait()
}
