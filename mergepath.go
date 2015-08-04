package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/leveldb"
	"github.com/golang/leveldb/db"
)

const (
	ERR_INVALID_PARAMS        = 1 << iota
	ERR_CREATE_TMP_DIR_FAILED = 1 << iota
	ERR_OPEN_DB_FAILED        = 1 << iota
)

type fileEntry struct {
	path string
	csum []byte
}

type PrintfFunc func(string, ...interface{}) (int, error)

var DEBUG PrintfFunc = func(fmt string, a ...interface{}) (int, error) { return 0, nil }

//var DEBUG PrintfFunc = fmt.Printf

func Copy(src, dst string) (err error) {
	var srcFd, dstFd *os.File
	if srcFd, err = os.Open(src); err != nil {
		return
	}

	defer srcFd.Close()

	if err = os.MkdirAll(filepath.Dir(dst), (os.ModeDir | 0750)); err != nil {
		return
	}

	// create a new file and ensure it doesn't exist (O_EXCL)
	if dstFd, err = os.OpenFile(dst, (os.O_WRONLY | os.O_CREATE | os.O_EXCL), 0640); err != nil {
		return
	}

	_, err = io.Copy(dstFd, srcFd)

	if errClose := dstFd.Close(); errClose != nil {
		if err != nil {
			return err
		} else {
			return errClose
		}
	}

	if err != nil {
		// If io.Copy() failed but one cannot remove newly created copy, what
		// can be done anymore?
		os.Remove(dst)
	}

	return
}

func processFile(dstPath string, dbh *leveldb.DB, processingCh <-chan *fileEntry, move bool) {
	var fent *fileEntry
	more := true

	for more {
		select {
		case fent, more = <-processingCh:
			if more {
				newPath := filepath.Join(dstPath, fent.path)

				if _, err := dbh.Get(fent.csum, nil); err == db.ErrNotFound {
					// file not processed yet
					if move {
						DEBUG("moving file %s to %s\n", fent.path, newPath)

						if err = os.MkdirAll(filepath.Dir(newPath), (os.ModeDir | 0750)); err != nil {
							fmt.Printf("%s\n", err)
							continue
						}

						if err = os.Rename(fent.path, newPath); err != nil {
							fmt.Printf("%s\n", err)
							continue
						}
					} else {
						DEBUG("copying file %s to %s\n", fent.path, newPath)
						if err = Copy(fent.path, newPath); err != nil {
							fmt.Printf("error: Couldn't copy file %s to %s: %s\n", fent.path, newPath, err)
							continue
						}
					}

					dbh.Set(fent.csum, []byte(fent.path), nil)
				} else if err == nil {
					DEBUG("skipping file %s since it already exists.\n", fent.path)
				} else {
					fmt.Printf("error: Unable to check whether %s exists: %s. Skipping.\n", fent.path, err)
					continue
				}
			}
		}
	}
}

func hashFile(path string) (csum []byte, err error) {
	var file *os.File
	hash := sha256.New()

	if file, err = os.Open(path); err != nil {
		return
	} else {
		// ignoring possible error in Close() on purpose; failed close on
		// O_RDONLY fd is not fatal (until file descriptors are consumed)
		defer file.Close()

		if _, err = io.Copy(hash, file); err != nil {
			return
		} else {
			csum = hash.Sum([]byte{})
		}
	}

	return
}

func hashFiles(hashingCh <-chan *fileEntry, processingCh chan<- *fileEntry) {
	defer close(processingCh)

	var fent *fileEntry
	var err error
	more := true

	for more {
		select {
		case fent, more = <-hashingCh:
			if more {
				if fent.csum, err = hashFile(fent.path); err != nil {
					fmt.Printf("error: Couldn't calculate checksum for file %s: %s\n", fent.path, err)
				} else {
					processingCh <- fent
				}
			}
		}
	}
}

func cleanupDbDir(dbDir string) {
	if err := os.RemoveAll(dbDir); err != nil {
		fmt.Printf("error: Unable to cleanup all temporary files from %s: %s", dbDir, err)
	}
}

func getPathProcessor(hashingCh chan<- *fileEntry) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			hashingCh <- &fileEntry{path: path}
		}

		return err
	}
}

func main() {
	PROG_NAME := filepath.Base(os.Args[0])

	var moveFiles bool
	flag.BoolVar(&moveFiles, "m", false, "move files to destination instead of copying")

	flag.Parse()

	if len(flag.Args()) < 2 {
		fmt.Printf("error: Invalid parameters\n")
		fmt.Printf("usage: %s [-m] dst src ...\n", PROG_NAME)
		os.Exit(ERR_INVALID_PARAMS)
	}

	// Initialize LevelDB
	var dbh *leveldb.DB

	if dbDir, err := ioutil.TempDir("", PROG_NAME); err != nil {
		fmt.Printf("error: Creation of tmp dir failed: %s\n", err)
		os.Exit(ERR_CREATE_TMP_DIR_FAILED)
	} else {
		defer cleanupDbDir(dbDir)
		dbh, err = leveldb.Open(dbDir, &db.Options{})

		if err != nil {
			fmt.Printf("error: Temporary working DB initialization failed: %s", err)
			os.Exit(ERR_OPEN_DB_FAILED)
		}

		defer dbh.Close()
	}

	// Create channels
	hashingCh := make(chan *fileEntry)
	defer close(hashingCh)

	processingCh := make(chan *fileEntry)

	// Go for it
	go hashFiles(hashingCh, processingCh)
	go processFile(flag.Args()[0], dbh, processingCh, moveFiles)

	for _, d := range flag.Args()[1:] {
		filepath.Walk(d, getPathProcessor(hashingCh))
	}
}
