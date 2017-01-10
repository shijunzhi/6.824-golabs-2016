package mapreduce

import (
	"fmt"
	"strconv"
	//"strings"
)

// Debugging enabled?
const debugEnabled = false

// DPrintf will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

type keyValueSorter []KeyValue

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func (kv keyValueSorter) Len() int {
	return len(kv)
}

func (kv keyValueSorter) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}

func (kv keyValueSorter) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}
