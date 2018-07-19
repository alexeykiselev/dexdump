package main

import (
	"flag"
	"log"
	"encoding/binary"
	"github.com/alexeykiselev/dexdump"
	"text/template"
	"text/tabwriter"
	"os"
	"time"
	"github.com/c2h5oh/datasize"
)

var keys = []string {
	"version",
	"height",
	"score",
	"block-at-height",
	"height-of",
	"waves-balance-history",
	"waves-balance",
	"assets-for-address",
	"asset-balance-history",
	"asset-balance",
	"asset-info-history",
	"asset-info",
	"lease-balance-history",
	"lease-balance",
	"lease-status-history",
	"lease-status",
	"filled-volume-and-fee-history",
	"filled-volume-and-fee",
	"transaction-info",
	"address-transaction-history",
	"address-transaction-ids-at-height",
	"changed-addresses",
	"transaction-ids-at-height",
	"address-id-of-alias",
	"last-address-id",
	"address-to-id",
	"id-of-address",
	"address-script-history",
	"address-script",
	"approved-features",
	"activated-features",
	"data-key-chunk-count",
	"data-key-chunk",
	"data-history",
	"data",
	"sponsorship-history",
	"sponsorship",
	"addresses-for-waves-seq-nr",
	"addresses-for-waves",
	"addresses-for-asset-seq-nr",
	"addresses-for-asset",
	"address-transaction-ids-seq-nr",
	"address-transaction-ids",
	"alias-is-disabled",
}

var idToKey = initKeys(keys)

func initKeys(keys []string) map[uint16]string {
	r := make(map[uint16]string)
	for i, k := range keys {
		r[uint16(i)] = k
	}
	return r
}

type stats struct {
	Key            string
	Count          int
	TotalKeySize   datasize.ByteSize
	TotalValueSize datasize.ByteSize
}

func main() {
	defer dexdump.TrackTime(time.Now(), "Collected")

	path := flag.String("node", "", "Path to node's LevelDB directory")
	flag.Parse()
	if *path == "" {
		flag.PrintDefaults()
		log.Fatalln("Invalid command line parameters")
	}

	db := dexdump.OpenDB(*path)
	defer db.Close()

	log.Println("Collecting DB stats")

	st := make(map[uint16]stats)

	it := db.NewIterator(nil, nil)
	for it.Next() {
		k := it.Key()
		vs := len(it.Value())
		id := binary.BigEndian.Uint16(k[:2])

		if _, ok := st[id]; !ok {
			st[id] = stats{Key: idToKey[id], Count: 0, TotalKeySize: 0, TotalValueSize: 0}
		}
		s := st[id]
		s.Count++
		s.TotalKeySize += datasize.ByteSize(len(k))
		s.TotalValueSize += datasize.ByteSize(vs)
		st[id] = s
	}
	it.Release()
	err := it.Error()
	if err != nil {
		log.Fatalf("LevelDB iterator error: %s", err)
	}
	printReport(st)
}

func printReport(st map[uint16]stats) {
	t := template.New("report")
	t, err := t.Parse("{{ range $k, $v := . }}{{$k}}\t{{$v.Key}}\t{{$v.Count}}\t{{$v.TotalKeySize.HR}}\t{{$v.TotalValueSize.HR}}\t\n{{end}}")
	if err != nil {
		log.Fatal(err)
	}
	w := tabwriter.NewWriter(os.Stdout, 4, 8, 4, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
	if err := t.Execute(w, st); err != nil {
		log.Fatal(err)
	}
	w.Flush()
}
