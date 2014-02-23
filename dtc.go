package dtc

import (
    "labix.org/v2/mgo"
)

var (
    session *mgo.Session
    db      *mgo.Database
    gridFS  *mgo.GridFS
)

func check(err error) {
    if err != nil {
        panic(err)
    }
}

func Connect(url, dbName, colName string) {
    var err error
    session, err = mgo.Dial(url)
    check(err)

    db = session.DB(dbName)

    gridFS = db.GridFS(colName)

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
