package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"time"
	"log"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strings"
	"github.com/mr-tron/base58/base58"
	"encoding/binary"
	"github.com/jinzhu/now"
	"sort"
	"flag"
	"github.com/alexeykiselev/dexdump"
)

const prefix = uint16(18)

type stats struct {
	week        time.Time
	total       int
	unprocessed int
}

type statsSlice []stats

func (s statsSlice) Len() int {
	return len(s)
}

func (s statsSlice) Less(i, j int) bool {
	return s[i].week.Before(s[j].week)
}

func (s statsSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func main() {
	defer dexdump.TrackTime(time.Now(), "Done")

	nodeDBPath := flag.String("node", "", "Path to node's LevelDB directory")
	matcherDBPath := flag.String("matcher", "", "Path to matcher's LevelDB directory")
	flag.Parse()
	if *nodeDBPath == "" || *matcherDBPath == "" {
		flag.PrintDefaults()
		log.Fatalln("Invalid command line parameters")
	}

	now.WeekStartDay = time.Monday
	p := make([]byte, 2)
	binary.BigEndian.PutUint16(p, uint16(prefix))

	ndb := dexdump.OpenDB(*nodeDBPath)
	defer ndb.Close()

	mdb := dexdump.OpenDB(*matcherDBPath)
	defer mdb.Close()

	weeks := make(map[time.Time]stats)
	mit := mdb.NewIterator(util.BytesPrefix([]byte("matcher:transactions")), nil)
	for mit.Next() {
		k := string(mit.Key())
		v := mit.Value()
		tm := now.New(extractTime(v))
		ws := tm.BeginningOfWeek()

		if _, ok := weeks[ws]; !ok {
			weeks[ws] = stats{week: ws, total: 0, unprocessed: 0}
		}
		s := weeks[ws]
		s.total++
		parts := strings.Split(k, ":")
		if len(parts) != 3 {
			log.Printf("Failed to get Key from the string '%s'", k)
			continue
		}
		key := parts[2]
		id, err := base58.Decode(key)
		if err != nil {
			log.Printf("Failed to decode transaction ID (%s): %s", k, err)
			continue
		}
		tid := append(p, id...)
		_, err = ndb.Get(tid, nil)
		if err != nil && err == leveldb.ErrNotFound {
			s.unprocessed++
		}
		weeks[ws] = s
	}
	mit.Release()
	err := mit.Error()
	if err != nil {
		log.Fatalf("LevelDB iterator error: %s", err)
	}
	printReport(weeks)
}

func printReport(weeks map[time.Time]stats) {
	stats := make(statsSlice, 0)
	for _, v := range weeks {
		stats = append(stats, v)
	}
	sort.Sort(stats)
	for _, s := range stats {
		we := now.New(s.week).EndOfWeek()
		fmt.Printf("%s - %s\n", s.week.Format("2006-01-02"), we.Format("2006-01-02"))
		fmt.Printf("\tUnprocessed transactions: %d\n", s.unprocessed)
		fmt.Printf("\tTotal transactions: %d\n", s.total)
		x := float32(s.unprocessed) / float32(s.total) * 100
		fmt.Printf("\tUnprocessed rate: %0.2f%%\n", x)
	}
}

func extractTime(b []byte) time.Time {
	const (
		signatureLength = 64
		timestampLength = 8
	)

	l := len(b)
	tsb := b[l-signatureLength-timestampLength : l-signatureLength]
	ts := int64(binary.BigEndian.Uint64(tsb) / 1000)
	return time.Unix(ts, 0)
}
