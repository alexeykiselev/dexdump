package dexdump

import (
	"time"
	"log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TrackTime(start time.Time, message string) {
	elapsed := time.Since(start)
	log.Printf("%s in %s", message, elapsed)
}

func OpenDB(path string) *leveldb.DB {
	o := &opt.Options{}
	o.ReadOnly = true
	o.ErrorIfMissing = true
	db, err := leveldb.OpenFile(path, o)
	if err != nil {
		log.Fatalf("Failed to open database '%s': %s", path, err)
	}
	return db
}
