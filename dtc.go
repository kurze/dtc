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

type StructFile struct {
	Name string
	File *os.File
}

func main() {
	var (
		channelFile chan StructFile
	)

	runtime.GOMAXPROCS(runtime.NumCPU())

	channelFile = make(chan StructFile, 1000)

	readFlag()
	//connect()

	w.Add(1)
	go scanDirByName(dirName, channelFile)
	w.Wait()
	close(channelFile)

	display(channelFile)
}

func display(cIn chan StructFile) {
	var (
		sf        StructFile
		notClosed bool
	)
	notClosed = true

	//	for f, ok = <-cIn; ok; {
	for notClosed {
		sf, notClosed = <-cIn
		log.Println(sf.Name)
	}
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

func isDir(dir *os.File) bool {
	var (
		err error
		fi  os.FileInfo
	)

	fi, err = dir.Stat()
	check(err)

	return fi.IsDir()
}

func scanDirByName(dirName string, cOut chan StructFile) {
	var (
		isD bool
		sf  StructFile
		err error
	)
	sf.Name = dirName
	sf.File, err = os.Open(dirName)
	check(err)
	isD = isDir(sf.File)
	if !isD {
		panic("not a directory")
	}

	scanDir(sf, cOut)
}

func scanDir(sf StructFile, cOut chan StructFile) {
	var (
		fileNames []string
		fileName  string
		err       error
		isD       bool
		newSf     StructFile
	)

	fileNames, err = sf.File.Readdirnames(0)
	check(err)

	for _, fileName = range fileNames {
		fileName = sf.Name + "/" + fileName
		newSf.Name = fileName
		newSf.File, err = os.Open(fileName)
		check(err)
		isD = isDir(newSf.File)
		if isD {
			w.Add(1)
			go scanDir(newSf, cOut)

		} else {
			cOut <- newSf
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
