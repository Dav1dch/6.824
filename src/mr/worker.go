package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const (
	ready   = 0
	working = 1
)

type worker struct {
	id      int
	status  int
	work    Work
	mapf    func(filename string, contents string) []KeyValue
	reducef func(key string, values []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func jsonName(seq int, index int) string {
	return fmt.Sprintf("mr-%d-%d", seq, index)
}

func outName(index int) string {
	return fmt.Sprintf("mr-out-%d", index)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.check()

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func (w *worker) check() {
	for {
		for w.status == working {
			time.Sleep(3 * time.Second)
		}
		w.getWork()
	}
}

func (w *worker) getWork() {
	args := WorkArgs{
		WorkerId: w.id,
	}
	reply := WorkReply{}
	call("Master.DistributeWork", &args, &reply)
	w.work = reply.Rwork
	w.status = working
	w.doWork()
}

func (w *worker) doWork() {
	switch w.work.WorkType {
	case mapWork:
		w.doMapWork()
	case reduceWork:
		w.doReduceWork()
	}
}

func (w *worker) doReduceWork() {
	var kva []KeyValue
	for i := 0; i < w.work.Nmap; i++ {
		filename := jsonName(i, w.work.Seq)
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	intermediate := kva
	sort.Sort(ByKey(intermediate))
	outFileName := outName(w.work.Seq)
	outF, _ := os.Create(outFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outF, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	args := w.work.Seq
	call("Master.ReportWork", &args, nil)
	w.status = ready
}

func (w *worker) doMapWork() {
	file, _ := os.Open(w.work.Filename)
	content, _ := ioutil.ReadAll(file)
	kva := w.mapf(w.work.Filename, string(content))
	reduces := make([][]KeyValue, w.work.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % w.work.NReduce
		reduces[index] = append(reduces[index], kv)
	}

	for index, r := range reduces {
		jFile, _ := os.Create(jsonName(w.work.Seq, index))
		enc := json.NewEncoder(jFile)
		for _, kv := range r {
			_ = enc.Encode(&kv)
		}
	}
	args := w.work.Seq
	call("Master.ReportWork", &args, nil)
	w.status = ready
}

func (w *worker) register() {
	args := WorkerArgs{}
	reply := WorkerReply{}
	call("Master.RegisterWorker", &args, &reply)
	w.id = reply.Id
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
