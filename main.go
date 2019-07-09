package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/diskqueue"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var (
	Version = "(UNKNOWN)"
)

func exit(err *error) {
	if *err != nil {
		log.Error().Err(*err).Msg("exited")
		os.Exit(1)
	}
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 5)
}

func main() {
	var (
		err         error
		hostname    string
		flagVersion bool
		flagVerbose bool

		optionsFile string
		options     Options

		queue diskqueue.DiskQueue

		stats *Stats

		input  Input
		output Output
	)

	defer exit(&err)

	// init logger
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: true, TimeFormat: time.RFC3339})

	// decode command line arguments
	flag.StringVar(&optionsFile, "c", "/etc/logtubed.yml", "config file")
	flag.BoolVar(&flagVerbose, "verbose", false, "enable verbose mode")
	flag.BoolVar(&flagVersion, "version", false, "show version")
	flag.Parse()

	if flagVersion {
		fmt.Println("logtubed " + Version)
		return
	}

	// load options
	log.Info().Str("file", optionsFile).Msg("load options file")
	if options, err = LoadOptions(optionsFile); err != nil {
		log.Error().Err(err).Msg("failed to load options file")
		return
	}

	// set verbose from command line arguments
	if flagVerbose {
		options.Verbose = true
	}

	// re-init logger
	if options.Verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	// configure mutex and block rate
	runtime.SetMutexProfileFraction(options.PProf.Mutex)
	runtime.SetBlockProfileRate(options.PProf.Block)

	// resolve hostname
	if hostname, err = os.Hostname(); err != nil {
		log.Error().Err(err).Msg("failed to retrieve hostname")
		return
	}

	// ensure data dir
	if err = os.MkdirAll(options.Queue.Dir, 0755); err != nil {
		log.Error().Err(err).Msg("failed to ensure xlog data dir")
		os.Exit(1)
		return
	}

	// create the queue
	queue = diskqueue.New(
		options.Queue.Name,
		options.Queue.Dir,
		256*1024*1024,
		20,
		2*1024*1024,
		int64(options.Queue.SyncEvery),
		time.Second*20,
	)

	log.Info().Str("name", options.Queue.Name).Msg("queue created")

	defer queue.Close() // close queue at last

	// create stats
	stats = NewStats(queue)
	go stats.Routine()

	// start debug http server, with pprof and stats
	http.HandleFunc("/stats", stats.Handler)
	go http.ListenAndServe(options.PProf.Bind, nil)

	// create outputs
	outputs := make([]Output, 0)

	if options.OutputES.Enabled {
		var output *ESOutput

		if output, err = NewESOutput(options.OutputES); err != nil {
			log.Error().Err(err).Msg("failed to create es output")
			return
		}
		log.Info().Msg("es output created")

		defer output.Close() // close output after input

		outputs = append(outputs, output)
	}

	if options.OutputLocal.Enabled {
		var output *LocalOutput

		if output, err = NewLocalOutput(options.OutputLocal); err != nil {
			log.Error().Err(err).Msg("failed to create local output")
			return
		}

		log.Info().Msg("local output created")

		defer output.Close() // close output after input

		outputs = append(outputs, output)
	}

	if len(outputs) == 0 {
		err = errors.New("no output")
		return
	}

	output = MultiOutput(outputs...)

	// create input
	inputs := make([]Input, 0)

	if options.InputRedis.Enabled {
		var input *RedisInput

		if input, err = NewRedisInput(options.InputRedis); err != nil {
			log.Error().Err(err).Msg("failed to create redis input")
			return
		}

		log.Info().Msg("redis input created")

		defer input.Close() // close input before output

		inputs = append(inputs, input)
	}

	if options.InputSPTP.Enabled {
		var input *SPTPInput

		if input, err = NewSPTPInput(options.InputSPTP); err != nil {
			log.Error().Err(err).Msg("failed to create SPTP input")
			return
		}

		log.Info().Msg("SPTP input created")

		defer input.Close() // close input before output

		inputs = append(inputs, input)
	}

	if len(inputs) == 0 {
		err = errors.New("no input")
		return
	}

	input = MultiInput(inputs...)

	// create buffer
	buffer := make(chan Event, 10000)

	waitSignal := make(chan os.Signal, 3)

	waitInput := make(chan bool, 1)
	go func() {
		err := input.Run(buffer)
		log.Info().Err(err).Msg("input routine exited")
		// notify input routine exited
		waitInput <- true
		// re-invoke global shutdown channel in case of early input failure
		waitSignal <- syscall.SIGUSR1
	}()

	shutTrans := make(chan bool, 1)
	waitTrans := make(chan bool, 1)
	go func() {
	forLoop:
		for {
			select {
			case e := <-buffer:
				var err error
				var buf []byte
				for _, t := range options.Topics.Ignored {
					if strings.ToLower(t) == strings.ToLower(e.Topic) {
						continue forLoop
					}
				}
				for _, t := range options.Topics.KeywordRequired {
					if strings.ToLower(t) == strings.ToLower(e.Topic) {
						if len(e.Keyword) == 0 {
							continue forLoop
						}
					}
				}
				// assign via
				e.Via = hostname
				// marshal operation
				if buf, err = e.ToOperation().DiskQueueMarshal(); err != nil {
					log.Error().Err(err).Msg("failed to marshal operation")
					continue forLoop
				}
				// put operation to diskqueue
				if err = queue.Put(buf); err != nil {
					log.Error().Err(err).Msg("failed to queue operation")
					continue forLoop
				}
				// increase stats
				stats.IncrQueueIn()
			case <-shutTrans:
				break forLoop
			}
		}
		log.Info().Msg("transform routine exited")
		waitTrans <- true
	}()

	shutOutput := make(chan bool, 1)
	waitOutput := make(chan bool, 1)
	go func() {
		out := queue.ReadChan()
	forLoop:
		for {
			select {
			case b := <-out:
				var err error
				var o Operation
				// increase stats
				stats.IncrQueueOut()
				// unmarshal operation
				if o, err = DiskQueueUnmarshal(b); err != nil {
					log.Error().Err(err).Msg("failed to unmarshal operation")
					continue forLoop
				}
				// output operation
				if err = output.Put(o); err != nil {
					log.Error().Err(err).Msg("failed to put output")
					continue forLoop
				}
			case <-shutOutput:
				break forLoop
			}
		}
		log.Info().Msg("output routine exited")
		waitOutput <- true
	}()

	// notify systemd for ready
	_, _ = SdNotify(false, SdNotifyReady)

	// wait for signal
	signal.Notify(waitSignal, syscall.SIGINT, syscall.SIGTERM)
	sig := <-waitSignal
	log.Info().Str("signal", sig.String()).Msg("signal caught")

	// notify systemd for stopping
	_, _ = SdNotify(false, SdNotifyStopping)

	// shutdown input
	_ = input.Close()
	// wait input
	<-waitInput
	log.Info().Msg("inputs exited")
	// close transform
	shutTrans <- true
	// wait transform
	<-waitTrans
	log.Info().Msg("transform exited")
	// close queue
	_ = queue.Close()
	// close output
	shutOutput <- true
	// wait output
	<-waitOutput
	log.Info().Msg("output exited")
}
