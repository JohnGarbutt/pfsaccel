package main

import (
	"fmt"
	"os/exec"
	"github.com/johngarbutt/pfsaccel/pkg/registry"
)

type BufferRegistry interface {
	Close() error
	ClearAllData()
	AddBuffer(id int)
	AddSlice(id int, s string)
    AddMountpoint(id string, mountpoint string)
    WatchNewBuffer(callback func(string, string))
	WatchNewSlice(callback func(key string, value string))
	WatchNewReady(callback func(key string, value string))
}

func main() {
	fmt.Println("Hello from pfsaccel demo.")

	var registry BufferRegistry = registry.NewBufferRegistry()
	defer registry.Close()

	// tidy up keys before we start and after we are finished
	registry.ClearAllData()
	defer registry.ClearAllData()

	// list of "available" slice ids
	slice_ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	slice_index := 0

	// watch for buffers, create slice on put
	make_slice := func(key string, value string) {
		registry.AddSlice(slice_ids[slice_index], key)
		slice_index++
	}
	// TODO: how to do context cancel on SIG_TERM
	go registry.WatchNewBuffer(make_slice)

	// watch for slice updates
	print_event := func(key string, value string) {
		buffer_key := value
		fakeMountpoint, err := exec.Command("date", "-u", "-Ins").Output()
		if err != nil{
			panic(err)
		}
		registry.AddMountpoint(buffer_key, string(fakeMountpoint))
	}
	go registry.WatchNewSlice(print_event)

	// watch for buffer setup complete
	print_buffer_ready := func(key string, value string) {
		fmt.Printf("Buffer ready %s with mountpoint %s", key, value)
	}
	go registry.WatchNewReady(print_buffer_ready)

	// add some fake buffers to test the watch
	ids := []int{1, 2, 3, 4, 5}
	for _, id := range ids {
		registry.AddBuffer(id)
	}
	registry.AddBuffer(16)
}
