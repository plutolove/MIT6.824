package mapreduce

import (
	"fmt"
	"sync"
)

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

	var mutex sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		mutex.Add(1)
		go func(taskid int, nfiles int, phase jobPhase) { // create a goroutine for a task
			defer mutex.Done()
			debug("DEBUG: current taskNum: %v, nios: %v, phase: %v\n", taskid, nios, phase)

			for {
				worker_ip := <- mr.registerChannel // get an unuse worker

				var args DoTaskArgs
				args.File = mr.files[taskid]
				args.JobName = mr.jobName
				args.NumOtherPhase = nios
				args.Phase = phase
				args.TaskNumber = taskid

				is_success := call(worker_ip, "Worker.DoTask", &args, new(struct{})) // run worker with rpc

				if is_success { // iif is success free the worker and put it in registerChannel, then break the loop
					go func() {
						mr.registerChannel <- worker_ip
					}()
					break
				}
			}

		}(i, nios, phase)
	}

	mutex.Wait()

	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
