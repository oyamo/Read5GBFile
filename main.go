package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type DataBlock [][]string
type BlockTypes map[string][]int

func (bt BlockTypes) BlockTypeCount(blocktype string) int {
	if _,ok := bt[blocktype]; ok {
		return len(bt[blocktype])
	}
	return 0
}

func (bt BlockTypes) MatchingBlocks(Data DataBlock, blocktype string) [][]string {
	if _,ok := bt[blocktype]; ok {
		out := make([][]string, len(bt[blocktype]))
		for i := 0; i < len(bt[blocktype]); i++ {
			out[i] = Data[bt[blocktype][i]]
		}
		return out
	}
	return nil
}

func ReadToString() (data []string, err error){
	var dbytes []byte
	startTime := time.Now()
	fmt.Printf("Started file read and split at %s\n",startTime.Format(time.UnixDate))
	dbytes,err = os.ReadFile("ripe.db")
	if err != nil {
		return nil, err
	}
	data = strings.Split(string(dbytes), "\n")
	fmt.Printf("Completed at %s [%s]\n",time.Now().Format(time.UnixDate), time.Since(startTime))
	return data, nil
}

func BreakToBlocks(in []string) (out DataBlock) {
	startTime := time.Now()
	var Start int
	haveStart := false
	fmt.Printf("Started locating data blocks at %s\n",startTime.Format(time.UnixDate))
	for i,x := range in  {
		if i >= len(in) {
			break
		}
		if !haveStart && len(x) == 0 && i < len(in)-1 && len(in[i+1]) > 0 && strings.Contains(in[i+1], ":") {
			Start = i+1
			haveStart = true
			continue
		}
		if haveStart && len(x) == 0 && i < len(in)-1 && len(in[i+1]) > 0 && strings.Contains(in[i+1], ":") {
			out = append(out, in[Start:i-1])
			haveStart = false
		}
	}
	fmt.Printf("Completed at %s [%s]\n",time.Now().Format(time.UnixDate), time.Since(startTime))
	return out
}

func (db DataBlock) ReturnBlockTypes() BlockTypes {
	var keys = make(map[string][]int)
	UpdateMap := func(data [][]string, out *map[string][]int, offset int, complete chan bool, mutex *sync.Mutex) {
		for blocknum, block := range data {
			MatchString := strings.Split(block[0], ":")[0]
			if (MatchString[0] >= 'a' && MatchString[0] <= 'z') || (MatchString[0] >= 'A' && MatchString[0] <= 'Z') {
				mutex.Lock()
				(*out)[MatchString] = append((*out)[MatchString], offset+blocknum)
				mutex.Unlock()
			}
		}
		complete <- true
	}
	var NumChans int
	Mutex := &sync.Mutex{}
	if len(db)%10 == 0 {
		NumChans = 10
	} else {
		NumChans = 11
	}
	CompleteChan := make(chan bool, NumChans)
	for i := 0; i < NumChans; i++ {
		start := i*(len(db)/10)
		end := (i*(len(db)/10))+len(db)/10
		if end > len(db) {
			end = len(db)-1
		}
		go UpdateMap(db[start:end], &keys, start, CompleteChan, Mutex)
	}
	for i := 0; i < NumChans; i++ {
		<- CompleteChan
	}
	return keys
}

func main() {
	StrData, _ := ReadToString()
	BlockOffsets  := BreakToBlocks(StrData)
	KeyPositions := BlockOffsets.ReturnBlockTypes()
	AsSet := KeyPositions.MatchingBlocks(BlockOffsets, "inet6num")
	for _,set := range AsSet {
		for _,str := range set {
			fmt.Printf("%s\n",str)
		}
	}
}
