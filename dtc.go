package main

import (
	"flag"
	"labix.org/v2/mgo"
	"log"
	"os"
	"runtime"
	"sync"
)

var (
	session *mgo.Session
	db      *mgo.Database
	gridFS  *mgo.GridFS

	url        string
	dbName     string
	gridFSName string
	dirName    string

	w sync.WaitGroup
)

func main() {
	var (
		channelFile chan *os.File
	)

	runtime.GOMAXPROCS(runtime.NumCPU())

	channelFile = make(chan *os.File, 1000)

	readFlag()
	//connect()

	w.Add(2)
	go scanDirByName(dirName, channelFile)
	go display(channelFile)
	w.Wait()
}

func display(cIn chan *os.File) {
	var (
		err error
		fi  os.FileInfo
		f   *os.File
		ok  bool
	)
	for f, ok = <-cIn; ok; {
		fi, err = f.Stat()
		check(err)
		log.Println(fi.Name())
	}
	w.Done()
}

func readFlag() {
	flag.StringVar(&url, "url", "localhost", "url of mongo server")
	flag.StringVar(&dbName, "db", "default", "name of the database")
	flag.StringVar(&gridFSName, "gridName", "fs", "name of the gridFS collection")
	flag.StringVar(&dirName, "dir", ".", "path to dir to send to the db")
	flag.Parse()
}

func connect() {
	var err error

	session, err = mgo.Dial(url)
	check(err)

	db = session.DB(dbName)

	gridFS = db.GridFS(gridFSName)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func isDir(dirName string) (bool, *os.File) {
	var (
		err   error
		f     *os.File
		fi    os.FileInfo
		count int
	)

	count = 0
	for count < 10 {
		f, err = os.Open(dirName)
		if err != nil {
			dirName, err = os.Readlink(dirName)
			check(err)
		} else {
			count = 10
		}
		count++
	}

	fi, err = f.Stat()
	check(err)

	return fi.IsDir(), f
}

func scanDirByName(dirName string, cOut chan *os.File) {
	var (
		isD bool
		f   *os.File
	)
	isD, f = isDir(dirName)
	if !isD {
		panic("not a directory")
	}
	scanDir(f, dirName, cOut)
	w.Done()
}

func scanDir(dir *os.File, dirName string, cOut chan *os.File) {
	var (
		fileNames []string
		fileName  string
		file      *os.File
		err       error
		isD       bool
	)

	fileNames, err = dir.Readdirnames(0)
	check(err)

	for _, fileName = range fileNames {
		fileName = dirName + "/" + fileName
		log.Println(fileName)
		isD, file = isDir(fileName)
		if isD {
			w.Add(1)
			go scanDir(file, fileName, cOut)
		} else {
			cOut <- file
		}
	}
	w.Done()
}

func GetGridFile(name string) *mgo.GridFile {
	var (
		err    error
		result *mgo.GridFile
	)

	result, err = gridFS.Open(name)
	check(err)

	return result

}

func GetFile(name string) []byte {
	var (
		result []byte
		file   *mgo.GridFile
		err    error
	)

	file = GetGridFile(name)
	result = make([]byte, file.Size())
	_, err = file.Read(result)
	check(err)
	return result

}
