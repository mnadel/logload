package main

import (
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

var server = flag.String("server", "", "mongod server/cluster address")
var database = flag.String("database", "", "mongodb database name")
var collection = flag.String("collection", "", "mongodb collection name")
var logdir = flag.String("logdir", "", "directory containing logfiles to import")
var logfile = flag.String("logfile", "", "logfile to import")

func main() {
	flag.Parse()

	if "" == *server {
		log.Fatal("Missing: -server")
	}

	if "" == *database {
		log.Fatal("Missing: -database")
	}

	if "" == *collection {
		log.Fatal("Missing: -collection")
	}

	if "" == *logdir && "" == *logfile {
		log.Fatal("Missing: -logdir and/or -logfile")
	}

	sess, err := mgo.Dial(*server)
	if err != nil {
		log.Panicf("Error dialing %s: %s", *server, err.Error())
	}
	defer sess.Close()

	ch := make(chan *LogRecord)
	var wg sync.WaitGroup

	files := getLogfiles()
	for _, file := range files {
		wg.Add(1)
		go func() {
			produceRecords(file, ch)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	coll := sess.DB(*database).C(*collection)
	inserted := 0

	for rec := range ch {
		err := coll.Insert(rec)
		if nil != err {
			log.Printf("Error inserting %v: %s", rec, err.Error())
		} else {
			inserted += 1
		}
		// fmt.Printf("Got record: %v", rec)
	}

	log.Printf("Inserted %d records", inserted)
}

func getLogfiles() []os.FileInfo {
	if "" == *logdir {
		stat, err := os.Stat(*logfile)
		if nil != err {
			log.Fatalf("Error stating %s: %s", *logfile, err.Error())
		}

		return []os.FileInfo{stat}
	} else if "" == *logfile {
		stats, err := ioutil.ReadDir(*logdir)
		if nil != err {
			log.Fatalf("Error reading %s: %s", *logdir, err.Error())
		}

		return stats
	} else {
		fullpath := fmt.Sprintf("%s/%s", *logdir, *logfile)
		stat, err := os.Stat(fullpath)
		if nil != err {
			log.Fatalf("Error stating %s: %s", fullpath, err.Error())
		}

		return []os.FileInfo{stat}
	}
}
