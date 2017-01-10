package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
	"fmt"
	"io"
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
	keyValues := readInterFiles(jobName, nMap, reduceTaskNumber)
	if len(keyValues) == 0 {
		return
	}
	debug("parse intermediate file, got %d pair key/value\n", len(keyValues))

	sort.Sort(keyValueSorter(keyValues))

	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeFileName)
	if err != nil {
		fmt.Printf("open merge file %s failed: %q\n", mergeFileName, err)
		return
	}
	defer mergeFile.Close()
	encoder := json.NewEncoder(mergeFile)

	var prevKey = keyValues[0].Key
	var values  = []string{}
	var reduceOutput string
	for _, kv := range keyValues {
		if prevKey == kv.Key {
			values = append(values, kv.Value)
		} else {
			reduceOutput = reduceF(prevKey, values)

			err = encoder.Encode(&KeyValue{prevKey, reduceOutput})
			if err != nil {
				fmt.Printf("encode merge file %s failed: %q\n", mergeFileName, err)
				return
			}

			// new key
			prevKey = kv.Key
			values = []string{}
			values = append(values, kv.Value)
		}
	}
	if len(values) != 0 {
		reduceOutput = reduceF(prevKey, values)
		err = encoder.Encode(&KeyValue{prevKey, reduceOutput})
		if err != nil {
			fmt.Printf("encode merge file %s failed: %q\n", mergeFileName, err)
			return
		}
	}
}

func readInterFiles(jobName string, nMap int, reduceTaskNumber int) []KeyValue {
	var keyValues = []KeyValue{}
	var kv KeyValue
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		interFile, err := os.Open(fileName)
		if (err != nil) && (!os.IsNotExist(err)) {
			fmt.Printf("read intermediate file %s failed: %q\n", fileName, err)
			continue
		}

		decoder := json.NewDecoder(interFile)
		for {
			err = decoder.Decode(&kv)
			if err != nil {
				if err != io.EOF {
					fmt.Printf("decode intermediate file %s failed: %q\n", fileName, err)
				}
				break
			}
			keyValues = append(keyValues, kv)
		}

		interFile.Close()
	}
	return keyValues
}
