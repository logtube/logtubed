package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"github.com/go-redis/redis"
	"github.com/logtube/logtubed/tools/recovergzlog/iocount"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
)

var (
	optDebug bool
	optRedis string

	linePattern = regexp.MustCompile(`^\[\d\d\d\d[/\-]\d\d[/\-]\d\d \d\d:\d\d:\d\d`)
	hostname, _ = os.Hostname()
)

type countReader struct {
	r io.Reader
	c int64
}

func (c *countReader) Read(p []byte) (n int, err error) {
	n, err = c.r.Read(p)
	atomic.AddInt64(&c.c, int64(n))
	return
}

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

	s := bufio.NewScanner(cr2)
	s.Split(bufio.ScanLines)

	var message string
	var count int64
	for s.Scan() {
		if count%100000 == 0 || optDebug {
			log.Printf("uncompressed: %s", iocount.SimpleFormatByteSize(cr2.Count()))
			log.Printf("compressed: %s", iocount.SimpleFormatByteSize(cr1.Count()))
		}
		line := s.Text()
		if linePattern.MatchString(line) {
			if len(message) != 0 {
				if err = submit(filename, message, client); err != nil {
					return
				}
			}
			message = line
		} else {
			message = message + "\n" + line
		}
		count++
	}
	if len(message) > 0 {
		if err = submit(filename, message, client); err != nil {
			return
		}
	}
	if err = s.Err(); err != nil {
		return
	}
	return
}

func submit(filename string, message string, client *redis.Client) (err error) {
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
