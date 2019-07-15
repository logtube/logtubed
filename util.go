package main

import (
	"errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

func combineError(err1 error, err2 error) error {
	if err1 == nil {
		if err2 == nil {
			return nil
		} else {
			return err2
		}
	} else {
		if err2 == nil {
			return err1
		} else {
			return errors.New(err1.Error() + "; " + err2.Error())
		}
	}
}

func setupZerolog(verbose bool) {
	if verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, NoColor: !verbose, TimeFormat: time.RFC3339})
}
