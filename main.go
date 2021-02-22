package main

import (
	"context"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"github.com/logtube/logtubed/core"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"io/ioutil"
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

func main() {
	var (
		err error

		optVersion bool
		optVerbose bool
		optCfgFile string
		optBlock   bool
		optUnblock bool

		opts types.Options

		outputEsStd core.ElasticOutput
		outputEsPri core.ElasticOutput

		queueStd    core.Queue
		queuePri    core.Queue
		outputLocal core.LocalOutput

		dispatcher types.EventConsumer

		inputRedis core.RedisInput
		inputSPTP  core.SPTPInput

		brOpts core.BlockRoutineOptions
		br     core.BlockRoutine
	)

	defer exit(&err)

	// init zerolog
	setupZerolog(false)

	// decode command line arguments
	flag.StringVar(&optCfgFile, "c", "/etc/logtubed.yml", "config file")
	flag.BoolVar(&optVerbose, "verbose", false, "enable verbose mode")
	flag.BoolVar(&optVersion, "version", false, "show version")
	flag.BoolVar(&optBlock, "block", false, "block running logtubed")
	flag.BoolVar(&optUnblock, "unblock", false, "unblock logtubed")
	flag.Parse()

	if optVersion {
		fmt.Println("logtubed " + Version)
		return
	}

	if optBlock {
		err = ioutil.WriteFile(core.BlockFile, []byte(core.BlockFileContent), 0644)
		return
	}

	if optUnblock {
		err = os.Remove(core.BlockFile)
		return
	}

	// load options
	log.Info().Str("file", optCfgFile).Msg("load options file")
	if opts, err = types.LoadOptions(optCfgFile); err != nil {
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
		if outputEsStd, err = core.NewElasticOutput(core.ElasticOutputOptions{
			Name:           "std",
			URLs:           opts.OutputES.URLs,
			NoSniff:        opts.OutputES.NoSniff,
			Concurrency:    opts.OutputES.Concurrency,
			BatchSize:      opts.OutputES.BatchSize,
			BatchTimeout:   time.Duration(opts.OutputES.BatchTimeout) * time.Second,
			NoMappingTypes: opts.OutputES.NoMappingTypes,
		}); err != nil {
			return
		}

		if queueStd, err = core.NewQueue(core.QueueOptions{
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
			if outputEsPri, err = core.NewElasticOutput(core.ElasticOutputOptions{
				Name:           "pri",
				URLs:           opts.OutputES.URLs,
				NoSniff:        opts.OutputES.NoSniff,
				Concurrency:    opts.OutputES.Concurrency,
				BatchSize:      opts.OutputES.BatchSize,
				BatchTimeout:   time.Duration(opts.OutputES.BatchTimeout) * time.Second,
				NoMappingTypes: opts.OutputES.NoMappingTypes,
			}); err != nil {
				return
			}

			if queuePri, err = core.NewQueue(core.QueueOptions{
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

		brOpts.Dirs = append(brOpts.Dirs, opts.Queue.Dir)
		brOpts.Watermarks = append(brOpts.Watermarks, opts.Queue.Watermark)
	}

	// initialize local output
	if opts.OutputLocal.Enabled {
		if outputLocal, err = core.NewLocalOutput(core.LocalOutputOptions{
			Dir: opts.OutputLocal.Dir,
		}); err != nil {
			return
		}

		brOpts.Dirs = append(brOpts.Dirs, opts.OutputLocal.Dir)
		brOpts.Watermarks = append(brOpts.Watermarks, opts.OutputLocal.Watermark)
	}

	// initialize dispatcher
	dOpts := core.DispatcherOptions{
		TopicIgnores:         opts.Topics.Ignored,
		TopicRequireKeywords: opts.Topics.KeywordRequired,
		KeywordIgnores:       opts.Keywords.Ingnored,
		Priors:               opts.Topics.Priors,
		Hostname:             opts.Hostname,
		Next:                 outputLocal,
		NextStd:              queueStd,
		NextPri:              queuePri,
		EnvMappings:          opts.Mappings.Env,
		TopicMappings:        opts.Mappings.Topic,
	}

	if dispatcher, err = core.NewDispatcher(dOpts); err != nil {
		return
	}

	// initialize Redis input
	if opts.InputRedis.Enabled {
		if inputRedis, err = core.NewRedisInput(core.RedisInputOptions{
			Bind:                   opts.InputRedis.Bind,
			Multi:                  opts.InputRedis.Multi,
			LogtubeTimeOffset:      opts.InputRedis.Pipeline.Logtube.TimeOffset,
			MySQLErrorIgnoreLevels: opts.InputRedis.Pipeline.MySQL.ErrorIgnoreLevels,
			Next:                   dispatcher,
		}); err != nil {
			return
		}

		brOpts.Blockables = append(brOpts.Blockables, inputRedis)
	}

	// initialize SPTP input
	if opts.InputSPTP.Enabled {
		if inputSPTP, err = core.NewSPTPInput(core.SPTPInputOptions{
			Bind: opts.InputSPTP.Bind,
			Next: dispatcher,
		}); err != nil {
			return
		}
	}

	// block routine
	br = core.NewBlockRoutine(brOpts)

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
	common.RunAsync(ctxL1, cancelL1, doneL1, inputSPTP, inputRedis, br)
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
