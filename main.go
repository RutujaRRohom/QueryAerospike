package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/aerospike/aerospike-client-go"
)

type record struct {
	Key     string
	Value1  string
	Value2  int
	Value3  string
	Value4  int
	Value5  string
	Value6  string
	Value7  string
	Value8  string
	Value9  string
	Value0  string
	Value11 string
	Value12 string
	Value13 string
	Value14 string
	Value15 string
}

type job struct {
	record record
}

func main() {
	fmt.Println("here we go")
	const numWorkers = 4

	client, err := aerospike.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
		return
	}

	records := []record{
		{"key23", "rutuja", 1, "dhdh", 3, "deye", "5-3-2020", "dwh", "whgege", "she", "hee", "shs", "shsh", "he", "ete", "jej"},
		{"key24", "rohom", 2, "eedh", 6, "dtwte", "4-8-2020", "cxc", "xeg", "hhu", "xdxd", "hhh", "ggg", "hhh", "hhh", "nnb"},

		{"key25", "dhh", 3, "weytdwty", 1, "ase", "2-10-2019", "vcv", "bbb", "bb", "u", "v", "h", "hh", "jb", "nn"},
		{"key26", "sb", 5, "heey", 2, "shgge", "5-10-2018", "vv", "er", "yt", "hh", "vv", "er", "yrf", "yf", "r"},
		{"key5", "its", 6, "dwy", 9, "ehege", "4-18-89", "ttr", "yy", "s", "y", "rr", "gg", "y", "h", "j"},
		{"key6", "raining", 7, "aswd", 8, "sdheh", "13-6-2020", "bb", "be", "vv", "rt", "hh", "tt", "ff", "kk", "sr"},
		{"keyR", "newone", 7, "aswd", 8, "sdheh", "13-6-2020", "bb", "be", "vv", "rt", "hh", "tt", "ff", "kk", "sr"},
	}

	workCh := make(chan job, numWorkers)
	resultsCh := make(chan error, numWorkers)

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go insertRecord(workCh, resultsCh, &wg, client)

	}

	for _, rec := range records {
		workCh <- job{record: rec}
	}

	close(workCh)

	// Start a goroutine to wait for them all to finish.
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	for err := range resultsCh {
		if err != nil {
			log.Println(err)
		}
	}

	client.Close()

}

func insertRecord(workCh <-chan job, resultsCh chan<- error, wg *sync.WaitGroup, client *aerospike.Client) {

	defer wg.Done()

	for j := range workCh {
		key, err := aerospike.NewKey("test1", "newset60", j.record.Key)
		if err != nil {
			resultsCh <- err
			return

		}
		bins := aerospike.BinMap{
			"BlockCode":     j.record.Value1,
			"Bin":           j.record.Value2,
			"Logo":          j.record.Value3,
			"IDApp":         j.record.Value4,
			"IDTxn":         j.record.Value5,
			"DateModified":  j.record.Value6,
			"DateMake":      j.record.Value7,
			"MakerID":       j.record.Value8,
			"DateCheck":     j.record.Value9,
			"CheckeraID":    j.record.Value0,
			"DateCreated":   j.record.Value11,
			"FlagMNTStatus": j.record.Value12,
			"MNTAction":     j.record.Value13,
			"MIGDate":       j.record.Value14,
			"ReqNo":         j.record.Value15,
		}
		fmt.Println("bins are------------>", bins)
		err = client.Put(nil, key, bins)
		if err != nil {
			resultsCh <- err
			return

		}

	}

}
