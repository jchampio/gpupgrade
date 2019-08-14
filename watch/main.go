package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
)

// Returned by handleEvents() when the watched directory changes.
var changedError error = errors.New("changes were detected in the watched directory")

func handleEvents(watcher *fsnotify.Watcher) error {
	var changed error = nil // set to changedError when any event is received

	// Exit on SIGINT.
	cancel := make(chan os.Signal, 1)
	signal.Notify(cancel, os.Interrupt)

MainLoop:
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				break MainLoop
			}

			// We saw a change. Log for debugging and cache the error for when
			// we're told to exit.
			log.Println(event)
			changed = changedError

		case err, ok := <-watcher.Errors:
			if !ok {
				break MainLoop
			}

			// Any errors during watch cause the program to exit immediately.
			return err

		case <-cancel:
			// Got a SIGINT. Closing the watcher will close its channels and
			// cause one of the other select cases to break from the loop.
			watcher.Close()
		}
	}

	return changed
}

func watch(dirs []string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// handleEvents() is the main loop for the application. It returns a single
	// error, which we'll wait on via channel before exiting. The only things
	// that can make handleEvents() return are a SIGINT or an underlying failure
	// in fsnotify.
	//
	// Note that handleEvents() executes in its own goroutine due to documented
	// requirements of the fsnotify implementation; otherwise we probably could
	// have avoided the channel/goroutine complexity.
	done := make(chan error)
	go func() {
		done <- handleEvents(watcher)
	}()

	// Add our directories to the watch.
	for _, dir := range dirs {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrapf(err, "error accessing path %s", path)
			}

			if info.IsDir() {
				if err := watcher.Add(path); err != nil {
					return errors.Wrapf(err, "failed to watch %s", path)
				}
			}

			return nil
		})

		if err != nil {
			return err
		}
	}

	// Now just wait for the main loop to do its thing.
	return <-done
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s DIR [DIR ...]\n", os.Args[0])
		os.Exit(1)
	}

	dirs := os.Args[1:]

	if err := watch(dirs); err != nil {
		log.Fatal(err)
	}
}
