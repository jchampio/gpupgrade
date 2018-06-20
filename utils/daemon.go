package utils

import (
	"fmt"
	"io"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type DaemonizableCommand interface {
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)
	Start() error
	Wait() error
}

func Daemonize(command DaemonizableCommand, output io.Writer, errput io.Writer, timeout time.Duration) error {
	// Open up pipes from the child process and start it.
	stderr, err := command.StderrPipe()
	if err != nil {
		return err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}

	err = command.Start()
	if err != nil {
		return err
	}

	// Spin up two goroutines to copy the entire contents of both pipes to our
	// output Writers.
	done := make(chan error)
	connectPipe := func(out io.Writer, in io.Reader, written *bool) {
		num, err := io.Copy(out, in)
		if written != nil {
			*written = (num > 0)
		}
		done <- err
	}

	var hadStderr bool
	go connectPipe(output, stdout, nil)
	go connectPipe(errput, stderr, &hadStderr)

	// Wait for both pipes to fully drain.
	for waiting := 2; waiting > 0; {
		select {
		case err := <-done:
			if err != nil {
				// TODO: anything?
				gplog.Error("Could not copy from child pipe: %v", err)
			}
			waiting--
		}
	}

	// If we got stderr, wait for the process to exit.
	if hadStderr {
		var timer <-chan time.Time
		if timeout > 0 {
			timer = time.After(timeout)
		}

		result := make(chan error, 1)
		go func() {
			result <- command.Wait()
		}()

		select {
		case err := <-result:
			return err
		case <-timer:
			// XXX We leave the Wait()ing goroutine to die. Is that okay? We
			// don't want to kill the process; it might be functional, and
			// that's for the user to decide...
			return fmt.Errorf("The daemon process reported an error but did not immediately exit. Review the logs before continuing.")
		}
	}

	return nil
}
