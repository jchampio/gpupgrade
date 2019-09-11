package main

import (
	"fmt"
	"os"
)

type Task interface {
	Run() (err error)
	// Revert() can be added in the future
	// ElapsedTime() can be added in the future
	// EstimatedTimeRemaining() can be added in the future
}

type First struct {
	oldBinDir string
	newBinDir string
}

func (i *First) Run() (err error) {
	fmt.Fprint(os.Stdout, "running\n")
	return nil
}
func (i *First) Revert() (err error) {
	fmt.Fprint(os.Stdout, "reverting\n")
	return nil
}

func RunAll(taskList []Task) {

	for _, task := range taskList {
		task.Run()
	}

}

func main() {
	taskList := []Task{&First{"foo", "bar"}, &First{"foo2", "bar2"}}
	RunAll(taskList)

	os.Exit(1)
}
