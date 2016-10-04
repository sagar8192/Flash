package main

import (
    "os"
    "sync"
    "time"

    "github.com/Sirupsen/logrus"
    "github.com/codegangsta/cli"

    "thrift_flash/server"
    "thrift_flash/collectors"
)

const (
    name    = "flash"
    version = "0.1.0"
    desc    = "Fast and lightweight proxy."
)

var log = logrus.WithFields(logrus.Fields{"app": "flash"})

func initLogrus() {
    logrus.SetFormatter(&logrus.TextFormatter{
        DisableColors:   true,
        TimestampFormat: time.RFC822,
        FullTimestamp:   true,
    })

    logrus.SetLevel(logrus.DebugLevel)

    logrus.SetOutput(os.Stdout)
}

func main() {
    app := cli.NewApp()
    app.Name = name
    app.Version = version
    app.Usage = desc
    app.Flags = []cli.Flag{
        cli.StringFlag{
            Name:  "log_level, l",
            Value: "info",
            Usage: "Logging level (debug, info, warn, error, fatal, panic)",
        },
    }
    app.Action = start
    app.Run(os.Args)
}

func start(ctx *cli.Context) {
    initLogrus()
    log.Info("Starting flash...")

    if ctx.Bool("profile") {
        log.Info("Profile set")
    }

    file_manager := server.Createfilewriter("flash_messages")

    var mainwg sync.WaitGroup
    mainwg.Add(2)

    go collectors.StartUdpCollector(&mainwg)
    go collectors.Scribeserver(&mainwg, file_manager)
    log.Info("Collectors have been started....")

    go server.Startdispatcher(&mainwg, file_manager)
    log.Info("Dispatcher has been started....")

    mainwg.Wait()
}
