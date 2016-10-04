package server

import (
    "fmt"
    "os"
    "strconv"
    "sync"
    "time"
)

type RotateWriter struct {
    lock                  sync.Mutex
    filename              string // should be set to the actual filename
    fp                    *os.File
    rotated_file_names    chan string
}

// Make a new RotateWriter. Return nil if error occurs during setup.
func Createfilewriter(filename string) *RotateWriter {
    w := &RotateWriter{filename: filename}
    w.rotated_file_names = make(chan string, 1000)

    err := w.Rotate()
    if err != nil {
        panic(err)
    }

    // We need to do fdatasync() to persist our data to disk
    go w.Flushtodisk(3)

    // We need to rotate the file at a regular interval
    go w.Rotateperiodically(3)
    return w
}

// Write satisfies the io.Writer interface.
func (w *RotateWriter) Write(output []byte) (int, error) {
    w.lock.Lock()
    defer w.lock.Unlock()
    return w.fp.Write(output)
}

// Write string to file.
func (w *RotateWriter) Writestring(output string) (int, error) {
    w.lock.Lock()
    defer w.lock.Unlock()
    return w.fp.WriteString(output)
}

// Perform the actual act of rotating and reopening file.
func (w *RotateWriter) Rotate() (err error) {
    w.lock.Lock()
    defer w.lock.Unlock()

    // Stat the file
    fstat, err := os.Stat(w.filename)
    if err != nil {
        fmt.Println("Could not open the file %s to stat", w.filename)
        os.Exit(1)
    }

    // We do not want to rotate the file if it empty and w.fp is assigned
    if fstat.Size()==0 && w.fp != nil{
        return
    }

    // Close existing file if open
    if w.fp != nil {
        err = w.fp.Close()
        w.fp = nil
        if err != nil {
            return
        }
    }

    // Rename dest file if it already exists
    _, err = os.Stat(w.filename)
    rotated_file_name := w.filename + "." + time.Now().Format(time.RFC3339) + "." + strconv.FormatInt(time.Now().UnixNano(), 10)
    if err == nil {
        err = os.Rename(w.filename, rotated_file_name)
        if err != nil {
            return
        }
    }

    // Send the rotated file name to dispatcher
    w.rotated_file_names<-rotated_file_name

    // Create a new file.
    w.Createfile()
    return
}


func (w *RotateWriter) Createfile() {
    fp, err := os.Create(w.filename)
    if err != nil {
        os.Exit(1)
    }
    w.fp = fp
}

func (w *RotateWriter) Flushtodisk(interval int) {
    for {
        time.Sleep(time.Millisecond * time.Duration(interval))
        w.fp.Sync()
    }
}

func (w *RotateWriter) Rotateperiodically(interval int) {
    for {
        time.Sleep(time.Millisecond * time.Duration(interval))
        w.Rotate()
    }
}
