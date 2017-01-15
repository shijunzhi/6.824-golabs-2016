package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var works = make([]string, 0)
	var scheduleChannel = make(chan *DoTaskArgs, ntasks)
	var failureChannel = make(chan *DoTaskArgs)
	var doneTasks = 0
	var taskArgs *DoTaskArgs
	for i := 0; i < ntasks; i++ {
		taskArgs = new(DoTaskArgs)
		taskArgs.JobName = mr.jobName
		taskArgs.Phase = phase
		taskArgs.TaskNumber = i
		taskArgs.NumOtherPhase = nios
		if phase == mapPhase {
			taskArgs.File = mr.files[i]
		}

		scheduleChannel <- taskArgs
	}

	for doneTasks < ntasks {
		select {
		case workAddr := <-mr.registerChannel:
			works = append(works, workAddr)
			go mr.scheduleTask(workAddr, scheduleChannel, failureChannel)
		case failureTask := <-failureChannel:
			if failureTask == nil {
				doneTasks++
			} else {
				scheduleChannel <- failureTask
			}
		}
	}
	close(scheduleChannel)

	if phase == mapPhase {
		go func() {
			for _, workAddr := range works {
				mr.registerChannel <- workAddr
			}
		}()
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) scheduleTask(workAddr string, scheduleChannel chan *DoTaskArgs,
	failureChannel chan *DoTaskArgs) {
	for {
		taskArgs, open := <-scheduleChannel
		if open {
			ok := call(workAddr, "Worker.DoTask", taskArgs, new(struct{}))
			if ok {
				failureChannel <- nil
			} else {
				fmt.Printf("run task %d on worker %s failed\n", taskArgs.TaskNumber, workAddr)
				failureChannel <- taskArgs
			}
		} else {
			break
		}
	}
}
