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
	var queue = []DoTaskArgs{}
	for t := 0; t < ntasks; t++ {
		task := DoTaskArgs{mr.jobName, "", phase, t, nios}
		if phase == mapPhase {
			task.File = mr.files[t]
		}
		queue = append(queue, task)
	}
	completed := make(chan int)
	for len(queue) > 0 {
		go func(task DoTaskArgs) {
			for w := range mr.registerChannel {
				if call(w, "Worker.DoTask", &task, new(struct{})) {
					completed <- 0
					mr.registerChannel <- w
					break
				}
				go func() {
					mr.registerChannel <- w
				}()
			}
		}(queue[0])
		queue = queue[1:]
	}
	for i := 0; i < ntasks; i++ {
		<-completed
		fmt.Printf("i:%d", i)
	}
	//
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
