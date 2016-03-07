package main

import (
    "os"
    "time"
    "Flash/server"

    "github.com/Sirupsen/logrus"
    "github.com/codegangsta/cli"
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
    log.Info("starting flash...")

    if ctx.Bool("profile") {
        log.Info("Profile set")
    }

    go server.Startcollector()

    log.Info("Collector has been started....")

    for {

    }
}
