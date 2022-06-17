package mgdb

import (
	"encoding/json"
	"fmt"
	"github.com/jcelliott/lumber"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...any)
		Error(string, ...any)
		Warn(string, ...any)
		Info(string, ...any)
		Debug(string, ...any)
		Trace(string, ...any)
	}

	Driver struct {
		mutex     sync.Mutex
		mutexes   map[string]*sync.Mutex
		directory string
		log       Logger
	}

	Options struct {
		Logger
	}
)

func New(directory string, options *Options) (*Driver, error) {
	directory = filepath.Clean(directory)

	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := Driver{
		directory: directory,
		mutexes:   make(map[string]*sync.Mutex),
		log:       opts.Logger,
	}

	if _, err := os.Stat(directory); err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", directory)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", directory)
	return &driver, os.MkdirAll(directory, 0755)
}

func (d *Driver) Write(collection, resource string, v any) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to save record")
	}

	if resource == "" {
		return fmt.Errorf("missing resource - unable to save record (no name)")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.directory, collection)
	file := filepath.Join(dir, resource+".json")
	tmpFile := file + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	bytes, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	bytes = append(bytes, byte('\n'))

	if err := ioutil.WriteFile(tmpFile, bytes, 0644); err != nil {
		return err
	}

	return os.Rename(tmpFile, file)
}

func (d *Driver) Read(collection, resource string, v any) error {
	if collection == "" {
		return fmt.Errorf("missing collection - unable to read")
	}

	if resource == "" {
		return fmt.Errorf("missing resource - unable to read record (no name)")
	}

	record := filepath.Join(d.directory, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}

	bytes, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("missing collection - unable to read")
	}

	dir := filepath.Join(d.directory, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir)

	var records []string

	for _, file := range files {
		bytes, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(bytes))
	}
	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {

	file := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.directory, file)

	switch fInfo, err := stat(dir); {
	case fInfo == nil, err != nil:
		return fmt.Errorf("unable to find file or directory named %v\n", file)

	case fInfo.Mode().IsDir():
		return os.RemoveAll(dir)

	case fInfo.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}
	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (fInfo os.FileInfo, err error) {
	if fInfo, err = os.Stat(path); os.IsNotExist(err) {
		fInfo, err = os.Stat(path + ".json")
	}
	return
}
