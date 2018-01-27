package daun

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
)

var Empty = io.EOF

type Database struct {
	File     *os.File
	Filename string
	Cache    *bytes.Buffer
}

// prepare encodes structs into buffers
func prepare(p interface{}) (bytes.Buffer, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		return buf, err
	}
	return buf, nil
}

// write writes data to disk
func (d Database) write(buf bytes.Buffer) error {
	w := bufio.NewWriter(d.File)
	_, err := w.Write(buf.Bytes())
	if err != nil {
		return err
	}
	w.Flush()
	return nil
}

// clanwrite is part of Delete(), which commits changes to disk
func (d Database) cleanwrite(data []byte) error {
	err := ioutil.WriteFile(d.Filename, data, 0600)
	if err != nil {
		return err
	}
	return nil
}

// binappend appends data to cache
func (d Database) binappend(binary bytes.Buffer) error {
	w := bufio.NewWriter(d.Cache)
	_, err := w.Write(binary.Bytes())
	if err != nil {
		return err
	}
	w.Flush()
	return nil
}

// binremove returns altered version of cache with the binary paremeter removed
func (d Database) binremove(binary bytes.Buffer) ([]byte, error) {
	buf, err := ioutil.ReadFile(d.Filename)
	if err != nil {
		return buf, err
	}
	return bytes.Replace(buf, binary.Bytes(), []byte(""), -1), nil
}

// replenish re-reads the table file and populates the cache
func (d Database) replenish() (*bytes.Buffer, error) {
	buf, err := ioutil.ReadFile(d.Filename)
	if err != nil {
		return bytes.NewBuffer(buf), err
	}
	return bytes.NewBuffer(buf), nil
}

// Open opens a database table. If it does not exist, it creates it.
func Open(filename string) (Database, error) {
	var d Database
	w, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return d, err
	}
	d.File = w
	d.Filename = filename

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return d, err
	}

	// save buffer to cache
	d.Cache = bytes.NewBuffer(buf)
	return d, nil
}

// Insert appends p into cache and disk.
func (d Database) Insert(p interface{}) error {
	binary, err := prepare(p)
	if err != nil {
		return err
	}
	err = d.binappend(binary)
	if err != nil {
		return err
	}
	d.write(binary)
	return nil
}

// Delete removes p from disk.
func (d Database) Delete(p interface{}) error {
	binary, err := prepare(p)
	if err != nil {
		return err
	}
	data, err := d.binremove(binary)
	if err != nil {
		return err
	}
	err = d.cleanwrite(data)
	if err != nil {
		return err
	}
	return nil
}

// Update first deletes old and then appends p to cache and disk.
func (d Database) Update(old interface{}, p interface{}) error {
	err := d.Delete(old)
	if err != nil {
		return err
	}
	err = d.Insert(p)
	if err != nil {
		return err
	}
	return nil
}

// Scan decodes p with the data of d.Cache.
func (d Database) Scan(p interface{}) (Database, error) {
	dec := gob.NewDecoder(d.Cache)
	err := dec.Decode(p)
	if err == io.EOF {
		d.Cache, err = d.replenish()
		if err != nil {
			return d, err
		}
		return d, Empty
	}
	if err != nil {
		return d, err
	}
	return d, nil
}
