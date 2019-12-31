package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"github.com/go-redis/redis"
	"github.com/logtube/logtubed/tools/recovergzlog/iocount"
	"github.com/logtube/logtubed/tools/recovergzlog/logline"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

var (
	optDebug bool
	optRedis string

	hostname, _ = os.Hostname()
)

type M map[string]interface{}

func exit(err *error) {
	if (*err) != nil {
		log.Printf("exited with error: %s", (*err).Error())
		os.Exit(1)
	} else {
		log.Println("exited")
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.BoolVar(&optDebug, "debug", false, "debug mode")
	flag.StringVar(&optRedis, "redis", "127.0.0.1:6379", "redis address")
	flag.Parse()

	client := redis.NewClient(&redis.Options{Addr: optRedis})
	if err = client.Ping().Err(); err != nil {
		return
	}

	var fis []os.FileInfo
	if fis, err = ioutil.ReadDir("."); err != nil {
		return
	}
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != ".gz" {
			continue
		}
		log.Printf("process: %s", fi.Name())
		var filename string
		if filename, err = filepath.Abs(fi.Name()); err != nil {
			return
		}
		if err = handleFile(filename, client); err != nil {
			return
		}
	}
}

func handleFile(filename string, client *redis.Client) (err error) {
	var f *os.File
	if f, err = os.OpenFile(filename, os.O_RDONLY, 0645); err != nil {
		return
	}
	defer f.Close()

	cr1 := iocount.NewReader(f)

	var gr *gzip.Reader
	if gr, err = gzip.NewReader(cr1); err != nil {
		return
	}
	defer gr.Close()

	cr2 := iocount.NewReader(gr)
	br := bufio.NewReader(cr2)

	var count int64
	cp := logline.NewComposer()

	for {
		// progress
		if count%100000 == 0 || optDebug {
			log.Printf("line: %d", count)
			log.Printf("uncompressed: %s", iocount.SimpleFormatByteSize(cr2.Count()))
			log.Printf("compressed: %s", iocount.SimpleFormatByteSize(cr1.Count()))
		}
		// read line
		line, err := br.ReadString('\n')
		// feed line
		if message := cp.Feed(line); len(message) > 0 {
			if err = handleMessage(filename, message, client); err != nil {
				return
			}
		}
		// break on error
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		// increase counter
		count++
	}
	// remaining message
	if message := cp.End(); len(message) > 0 {
		if err = handleMessage(filename, message, client); err != nil {
			return
		}
	}
	return
}

func handleMessage(filename string, message string, client *redis.Client) (err error) {
	m := M{
		"beat": M{
			"hostname": hostname,
		},
		"message": message,
		"source":  filename,
	}
	var buf []byte
	if buf, err = json.Marshal(m); err != nil {
		return
	}
	err = client.RPush("xlog", string(buf)).Err()
	return
}
