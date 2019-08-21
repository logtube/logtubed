package main

import (
	"context"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"github.com/logtube/logtubed/internal"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
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

func setupZerolog(verbose bool) {
	if verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: !verbose, TimeFormat: time.RFC3339})
}

// loadOptions load options from yaml file
func loadOptions(filename string) (opt Options, err error) {
	if err = common.LoadYAMLConfigFile(filename, &opt); err != nil {
		if os.IsNotExist(err) {
			if err = common.SetDefaults(&opt); err != nil {
				return
			}
			log.Info().Str("filename", filename).Msg("config file not found, loading from defaults and envs")
		} else {
			return
		}
	}
	if len(opt.Hostname) == 0 {
		opt.Hostname, _ = os.Hostname()
	}
	if len(opt.Hostname) == 0 {
		opt.Hostname = "localhost"
	}
	return
}

func main() {
	var (
		err error

		optVersion bool
		optVerbose bool
		optCfgFile string

		opts Options

		outputEsStd internal.ElasticOutput
		outputEsPri internal.ElasticOutput

		queueStd    internal.Queue
		queuePri    internal.Queue
		outputLocal internal.LocalOutput

		dispatcher types.EventConsumer

		inputRedis internal.RedisInput
		inputSPTP  internal.SPTPInput
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
	if opts, err = loadOptions(optCfgFile); err != nil {
		log.Error().Err(err).Msg("failed to load options file")
		return
	}

	// adjust options.Verbose and re-init zerolog if needed
	if opts.Verbose = opts.Verbose || optVerbose; opts.Verbose {
		setupZerolog(true)
	}

	log.Info().Interface("options", opts).Msg("options loaded")

	// configure mutex and block rate
	runtime.SetMutexProfileFraction(opts.PProf.Mutex)
	runtime.SetBlockProfileRate(opts.PProf.Block)

	// initialize elastic output, and associated queues
	if opts.OutputES.Enabled {
		if outputEsStd, err = internal.NewElasticOutput(internal.ElasticOutputOptions{
			Name:         "std",
			URLs:         opts.OutputES.URLs,
			Concurrency:  opts.OutputES.Concurrency,
			BatchSize:    opts.OutputES.BatchSize,
			BatchTimeout: time.Duration(opts.OutputES.BatchTimeout) * time.Second,
		}); err != nil {
			return
		}

		if queueStd, err = internal.NewQueue(internal.QueueOptions{
			Dir:       opts.Queue.Dir,
			Name:      opts.Queue.Name,
			SyncEvery: opts.Queue.SyncEvery,
			Next:      outputEsStd,
			VarInput:  expvar.NewInt("queue-std-input"),
			VarOutput: expvar.NewInt("queue-std-output"),
			VarDepth:  expvar.NewInt("queue-std-depth"),
		}); err != nil {
			return
		}

		if len(opts.Topics.Priors) > 0 {
			if outputEsPri, err = internal.NewElasticOutput(internal.ElasticOutputOptions{
				Name:         "pri",
				URLs:         opts.OutputES.URLs,
				Concurrency:  opts.OutputES.Concurrency,
				BatchSize:    opts.OutputES.BatchSize,
				BatchTimeout: time.Duration(opts.OutputES.BatchTimeout) * time.Second,
			}); err != nil {
				return
			}

			if queuePri, err = internal.NewQueue(internal.QueueOptions{
				Dir:       opts.Queue.Dir,
				Name:      opts.Queue.Name + "-pri",
				SyncEvery: opts.Queue.SyncEvery,
				Next:      outputEsPri,
				VarInput:  expvar.NewInt("queue-pri-input"),
				VarOutput: expvar.NewInt("queue-pri-output"),
				VarDepth:  expvar.NewInt("queue-pri-depth"),
			}); err != nil {
				return
			}
		}
	}

	// initialize local output
	if opts.OutputLocal.Enabled {
		if outputLocal, err = internal.NewLocalOutput(internal.LocalOutputOptions{
			Dir: opts.OutputLocal.Dir,
		}); err != nil {
			return
		}
	}

	// initialize dispatcher
	dOpts := internal.DispatcherOptions{
		Ignores:  opts.Topics.Ignored,
		Keywords: opts.Topics.KeywordRequired,
		Priors:   opts.Topics.Priors,
		Hostname: opts.Hostname,
		Next:     outputLocal,
		NextStd:  queueStd,
		NextPri:  queuePri,
	}

	if dispatcher, err = internal.NewDispatcher(dOpts); err != nil {
		return
	}

	// initialize Redis input
	if opts.InputRedis.Enabled {
		if inputRedis, err = internal.NewRedisInput(internal.RedisInputOptions{
			Bind:       opts.InputRedis.Bind,
			Multi:      opts.InputRedis.Multi,
			TimeOffset: opts.InputRedis.TimeOffset,
			Next:       dispatcher,
		}); err != nil {
			return
		}
	}

	// initialize SPTP input
	if opts.InputSPTP.Enabled {
		if inputSPTP, err = internal.NewSPTPInput(internal.SPTPInputOptions{
			Bind: opts.InputSPTP.Bind,
			Next: dispatcher,
		}); err != nil {
			return
		}
	}

	// contexts
	ctxL3, cancelL3 := context.WithCancel(context.Background())
	doneL3 := make(chan error)

	ctxL2, cancelL2 := context.WithCancel(ctxL3)
	doneL2 := make(chan error)

	ctxL1, cancelL1 := context.WithCancel(ctxL2)
	doneL1 := make(chan error)

	// ignite L3
	log.Info().Msg("L3 ignite")
	common.RunAsync(ctxL3, cancelL3, doneL3, outputEsStd, outputEsPri)
	time.Sleep(time.Millisecond * 100)

	// ignite L2
	log.Info().Msg("L2 ignite")
	common.RunAsync(ctxL2, cancelL2, doneL2, queuePri, queueStd, outputLocal)
	time.Sleep(time.Millisecond * 100)

	// ignite L1
	if inputSPTP == nil && inputRedis == nil {
		log.Info().Msg("no inputs, running in drain mode")
	}
	log.Info().Msg("L1 ignite")
	common.RunAsync(ctxL1, cancelL1, doneL1, inputSPTP, inputRedis, common.DummyRunnable)
	time.Sleep(time.Millisecond * 100)

	// ignite pprof / expvar
	go http.ListenAndServe(opts.PProf.Bind, nil)

	// notify systemd
	_, _ = common.SdNotify(false, common.SdNotifyReady)

	// signal ch
	chsig := make(chan os.Signal, 3)
	signal.Notify(chsig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-doneL1:
		err = errors.New("inputs quited unexpected")
		log.Error().Err(err).Msg("error occurred")
	case sig := <-chsig:
		log.Info().Str("signal", sig.String()).Msg("signal caught")
	}

	// notify systemd
	_, _ = common.SdNotify(false, common.SdNotifyStopping)

	// cancel L1
	cancelL1()
	if err = <-doneL1; err != nil {
		log.Error().Err(err).Msg("L1 cut off failed")
	} else {
		log.Info().Msg("L1 cut off")
	}

	// cancel L2
	cancelL2()
	if err = <-doneL2; err != nil {
		log.Error().Err(err).Msg("L2 cut off failed")
	} else {
		log.Info().Msg("L2 cut off")
	}

	// cancel L3
	cancelL3()
	if err = <-doneL3; err != nil {
		log.Error().Err(err).Msg("L3 cut off failed")
	} else {
		log.Info().Msg("L3 cut off")
	}
}
