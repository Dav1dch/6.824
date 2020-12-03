package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	mapWork    = 0
	reduceWork = 1
)

const (
	inqueue     = 0
	readytowork = 1
	running     = 2
	finished    = 3
	err         = 4
)

const MaxRunningTime = time.Second * 10

type Work struct {
	WorkType int
	Seq      int
	NReduce  int
	Nmap     int
	Filename string
}

type WorkState struct {
	WorkerId  int
	Phase     int
	startTime time.Time
}

type Master struct {
	// Your definitions here.
	nWorker    int
	nMap       int
	nReduce    int
	state      int
	status     int
	done       bool
	files      []string
	worksState []WorkState
	workch     chan Work
	mutex      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RegisterWorker(args *WorkerArgs, reply *WorkerReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	reply.Id = m.nWorker
	m.nWorker += 1
	return nil
}

func (m *Master) DistributeWork(args *WorkArgs, reply *WorkReply) error {
	for m.status == ready || len(m.workch) < 1 {
		time.Sleep(3 * time.Second)
	}
	w := <-m.workch
	m.worksState[w.Seq].Phase = running
	m.worksState[w.Seq].WorkerId = args.WorkerId
	m.worksState[w.Seq].startTime = time.Now()
	reply.Rwork = w
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.done

	// Your code here.
	return ret
}

func (m *Master) initMapWork() {
	m.worksState = make([]WorkState, m.nMap)
	m.workch = make(chan Work, m.nMap)
	m.state = mapWork
	m.status = ready
}

func (m *Master) initReduceWork() {
	m.worksState = make([]WorkState, m.nReduce)
	m.workch = make(chan Work, m.nReduce)
	m.state = reduceWork
	m.status = ready

}

func (m *Master) regWork(index int) Work {
	w := Work{
		WorkType: m.state,
		NReduce:  m.nReduce,
		Nmap:     m.nMap,
		Seq:      index,
	}
	if m.state == mapWork {
		w.Filename = m.files[index]
	}
	return w
}

func (m *Master) ReportWork(args *int, reply *int) error {
	m.worksState[*args].Phase = finished
	return nil
}

func (m *Master) check() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	allFinished := true
	for index, s := range m.worksState {
		switch s.Phase {
		case inqueue:
			allFinished = false
			m.workch <- m.regWork(index)
			m.worksState[index].Phase = readytowork
			m.status = working
		case readytowork:
			allFinished = false
		case running:
			allFinished = false
			if time.Now().Sub(s.startTime) > MaxRunningTime {
				m.workch <- m.regWork(index)
				m.worksState[index].Phase = readytowork
			}
		case finished:
		default:
		}
	}
	if allFinished {
		if m.state == mapWork {
			m.initReduceWork()
		} else {
			m.DoneWork()
		}
	}
}

func (m *Master) DoneWork() {
	m.done = true
}

func (m *Master) loop() {
	for {
		go m.check()
		time.Sleep(5 * time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nWorker = 0
	m.nReduce = nReduce
	m.nMap = len(files)
	m.files = files
	m.initMapWork()

	go m.loop()
	// Your code here.
	m.server()
	return &m
}
