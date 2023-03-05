package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	interFileNum = 10
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KVSlice []KeyValue

func (s KVSlice) Len() int {
	return len(s)
}

func (s KVSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s KVSlice) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	// map
	mapJobNum := getMapNum()
	for {
		mapFile, mapJobID := getMapJob()
		if strings.Compare(mapFile, "") != 0 {
			file, err := os.Open(mapFile)
			if err != nil {
				log.Fatalf("cannot open %v", mapFile)
			}
			content, err := io.ReadAll(file)
			// 读取文件内容
			if err != nil {
				log.Fatalf("cannot read %v", mapFile)
			}
			file.Close()
			kvs := mapf(mapFile, string(content))
			shuffle(mapJobID, kvs)
			setMapJobDone(mapFile)
		} else {
			// 全部map任务执行完成
			if getAllMapJobDone() {
				break
			}
			time.Sleep(time.Second)
		}
	}
	// reduce
	for {
		reduceJobID := getReduceJob()
		if reduceJobID != -1 {
			kvs := make([]KeyValue, 0)
			for i := 0; i < mapJobNum; i++ {
				fileName := fmt.Sprintf("m-%d-%d", i, reduceJobID)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						log.Fatal("json decoder decode failed")
					}
					kvs = append(kvs, kv)
				}
			}
			storeReduceRes(reduceJobID, kvs, reducef)
			getReduceJobDone(reduceJobID)
		} else {
			if getMROver() {
				break
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// getMapNum 获取map任务数量
func getMapNum() int {
	args := MapNumArgs{}
	reply := MapNumReply{}
	ok := call("Coordinator.MapJobNum", &args, &reply)
	if !ok {
		log.Fatal("call MapJobNum failed")
	}
	return reply.Num
}

// getMapJob 获取新的map任务
func getMapJob() (string, int) {
	args := MapJobArgs{}
	reply := MapJobReply{}
	ok := call("Coordinator.MapJob", &args, &reply)
	if !ok {
		log.Fatal("call MapJob failed")
	}
	return reply.File, reply.MapJobID
}

// setMapJobDone 通知coordinator map一个任务已经完成
func setMapJobDone(fileName string) {
	args := MapJobDoneArgs{FileName: fileName}
	reply := MapJobDoneReply{}
	ok := call("Coordinator.SingleMapJobDone", &args, &reply)
	if !ok {
		log.Fatal("call SingleMapJobDone failed")
	}
}

// getAllMapJobDone 询问coordinator map全部任务是否已经完成
func getAllMapJobDone() bool {
	args := MapJobDoneArgs{}
	reply := MapJobDoneReply{}
	ok := call("Coordinator.AllMapJobDone", &args, &reply)
	if !ok {
		log.Fatal("call SingleMapJobDone failed")
	}
	return reply.Done
}

// shuffle 将map的结果保存至中间文件
func shuffle(mapJobID int, intermediate []KeyValue) {
	interFiles := make([]*os.File, interFileNum)
	var err error
	for i := 0; i < 10; i++ {
		interFileName := fmt.Sprintf("m-%d-%d", mapJobID, i)
		interFiles[i], err = os.Create(interFileName)
		if err != nil {
			log.Fatalf("create %v file failed", interFileName)
		}
		interFiles[i].Close()
	}
	for _, kv := range intermediate {
		reduceJobID := ihash(kv.Key) % interFileNum
		encoder := json.NewEncoder(interFiles[reduceJobID])
		err = encoder.Encode(&kv)
		if err != nil {
			log.Fatal("json encoder encode failed")
		}
	}
}

// getReduceJob 获取reduce
func getReduceJob() int {
	args := ReduceArgs{}
	reply := ReduceReply{}
	ok := call("Coordinator.ReduceJob", &args, &reply)
	if !ok {
		log.Fatal("call ReduceJob failed")
	}
	return reply.ReduceJobID
}

// storeReduceRes 将reduce任务执行结果保存在最终的文件中
func storeReduceRes(reduceJobID int, intermediate []KeyValue, reducef func(string, []string) string) {
	fileName := fmt.Sprintf("r-out-%d", reduceJobID)
	file, _ := os.Create(fileName)
	defer file.Close()
	sort.Sort(KVSlice(intermediate))
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

// getReduceJobDone 通知某个reduce任务完成
func getReduceJobDone(jobID int) {
	args := ReduceJobDoneArgs{JobID: jobID}
	reply := ReduceJobDoneReply{}
	ok := call("Coordinator.ReduceJobDone", &args, &reply)
	if !ok {
		log.Fatal("call ReduceJobDone failed")
	}
}

// getMROver 询问是否全部map-reduce任务完成
func getMROver() bool {
	args := MROverArgs{}
	reply := MROverReply{}
	ok := call("Coordinator.MROver", &args, &reply)
	if !ok {
		log.Fatal("call MROver failed")
	}
	return reply.Done
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
