package mapreduce

import (
	"os"
	"fmt"
	"encoding/json"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	kvs := make(map[string][]string) // read the reduce file
	for i := 0; i < nMap; i++ {
		fname := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fname)
		if err != nil {
			fmt.Println(err)
			return
		} else {
			json_decoder := json.NewDecoder(file)
			for {
				var item KeyValue
				err := json_decoder.Decode(&item)
				if err != nil {
					break
				}
				_, isexist := kvs[item.Key]
				if !isexist {
					kvs[item.Key] = make([]string, 0)
				}
				kvs[item.Key] = append(kvs[item.Key], item.Value)
			}
			file.Close()
		}
	}



	keys := make([]string, 0) // get all keys and sort them
	for item, _ := range kvs {
		keys = append(keys, item)
	}
	sort.Strings(keys)

	out_file, err := os.Create(mergeName(jobName, reduceTaskNumber))

	if err != nil {
		fmt.Println(err)
		return
	}

	json_encoder := json.NewEncoder(out_file) //merge and output the result

	for _, item := range keys {
		json_encoder.Encode(KeyValue{item, reduceF(item, kvs[item])})
	}
	defer out_file.Close()
}
