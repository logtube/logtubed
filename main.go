package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/rs/zerolog/log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
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
		err error

		optVersion bool
		optVerbose bool
		optCfgFile string

		options Options

		outputs MultiOutput
		queue   Queue
		inputs  MultiInput
	)

	defer exit(&err)

	// init zerolog
	setupZerolog(false)

	// decode command line arguments
	flag.StringVar(&optCfgFile, "c", "/etc/logtubed.yml", "config file")
	flag.BoolVar(&optVerbose, "verbose", false, "enable verbose mode")
	flag.BoolVar(&optVersion, "version", false, "show version")
	flag.Parse()

	if optVersion {
		fmt.Println("logtubed " + Version)
		return
	}

	// load options
	log.Info().Str("file", optCfgFile).Msg("load options file")
	if options, err = LoadOptions(optCfgFile); err != nil {
		log.Error().Err(err).Msg("failed to load options file")
		return
	}

	// adjust options.Verbose and re-init zerolog if needed
	if options.Verbose = options.Verbose || optVerbose; options.Verbose {
		setupZerolog(true)
	}

	// configure mutex and block rate
	runtime.SetMutexProfileFraction(options.PProf.Mutex)
	runtime.SetBlockProfileRate(options.PProf.Block)

	// create outputs
	if options.OutputES.Enabled {
		var output *ESOutput
		if output, err = NewESOutput(options.OutputES); err != nil {
			log.Error().Err(err).Msg("failed to create es output")
			return
		}
		log.Info().Msg("es output created")
		outputs = append(outputs, output)
	}

	if options.OutputLocal.Enabled {
		var output *LocalOutput
		if output, err = NewLocalOutput(options.OutputLocal); err != nil {
			log.Error().Err(err).Msg("failed to create local output")
			return
		}
		log.Info().Msg("local output created")
		outputs = append(outputs, output)
	}

	if len(outputs) == 0 {
		err = errors.New("no output")
		return
	}
	defer outputs.Close()

	// create the queue
	if queue, err = NewEventQueue(options.Queue, options.Topics); err != nil {
		log.Error().Err(err).Msg("failed to create queue")
		return
	}
	log.Info().Str("name", options.Queue.Name).Str("dir", options.Queue.Dir).Msg("queue created")

	// register http handle func
	http.HandleFunc("/stats", queue.Stats().Handler)

	// create input
	if options.InputRedis.Enabled {
		var input *RedisInput
		if input, err = NewRedisInput(options.InputRedis); err != nil {
			log.Error().Err(err).Msg("failed to create redis input")
			return
		}
		log.Info().Msg("redis input created")
		inputs = append(inputs, input)
	}

	if options.InputSPTP.Enabled {
		var input *SPTPInput
		if input, err = NewSPTPInput(options.InputSPTP); err != nil {
			log.Error().Err(err).Msg("failed to create SPTP input")
			return
		}
		log.Info().Msg("SPTP input created")
		inputs = append(inputs, input)
	}

	if len(inputs) == 0 {
		log.Info().Msg("no inputs, running in drain mode")
		inputs = append(inputs, DummyInput{})
	}

	// queue ignition
	queueCtx, queueCancel := context.WithCancel(context.Background())
	queueDone := make(chan interface{})

	go func() {
		err = queue.Run(queueCtx, outputs)
		close(queueDone)
	}()

	// inputs ignition
	inputsCtx, inputsCancel := context.WithCancel(context.Background())
	inputsDone := make(chan interface{})

	go func() {
		err = inputs.Run(inputsCtx, inputsCancel, queue)
		close(inputsDone)
	}()

	// pprof and stats ignition
	go http.ListenAndServe(options.PProf.Bind, nil)

	// notify systemd for ready
	_, _ = SdNotify(false, SdNotifyReady)

	// wait for signal
	sigch := make(chan os.Signal, 3)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-inputsDone:
		err = errors.New("inputs quited unexpected")
		log.Error().Err(err).Msg("inputs exited unexpected")
	case sig := <-sigch:
		log.Info().Str("signal", sig.String()).Msg("signal caught")
	}

	// notify systemd for stopping
	_, _ = SdNotify(false, SdNotifyStopping)

	// close inputs
	inputsCancel()
	<-inputsDone
	log.Info().Msg("inputs closed")

	// close queue
	queueCancel()
	<-queueDone
	log.Info().Msg("queue closed")
}
