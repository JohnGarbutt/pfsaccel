package main

import (
	"fmt"
	"container/list"
	"os/exec"
	"./registry"
)

type Keystore interface {
	Close() error
	CleanPrefix(prefix string)
	AtomicAdd(key string, value string)
	WatchPrefix(prefix string, onPut func(string, string))
}

func clearAllData(client Keystore) {
	fmt.Println("Cleanup started")
	client.CleanPrefix("/buffer")
	client.CleanPrefix("/slice")
	client.CleanPrefix("/ready")
	fmt.Println("Cleanup done")
}

func main() {
	fmt.Println("Hello from pfsaccel demo.")

	var cli Keystore = registry.NewKeystore()
	defer cli.Close()

	// tidy up keys before we start and after we are finished
	clearAllData(cli)
	defer clearAllData(cli)

	var atomicAddBuffer = func(id int) {
		var  key = fmt.Sprintf("/buffer/%d", id)
		cli.AtomicAdd(key, "I am a new buffer")
	}

	// list of "available" slice ids
	slice_list := list.New()
	slice_ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, id := range slice_ids {
		slice_list.PushBack(id)
	}
	slice_list_next := slice_list.Front()

	// watch for buffers, create slice on put
	make_slice := func(key string, value string) {
		cli.AtomicAdd(fmt.Sprintf("/slice/%d", slice_list_next.Value), string(key))
		slice_list_next = slice_list_next.Next()
	}
	go cli.WatchPrefix("/buffer", make_slice)

	// watch for slice updates
	print_event := func(key string, value string) {
		buffer_key := value
		fakeMountpoint, err := exec.Command("date", "-u", "-Ins").Output()
		if err != nil{
			panic(err)
		}
		cli.AtomicAdd(fmt.Sprintf("/ready%s", buffer_key), string(fakeMountpoint))
	}
	go cli.WatchPrefix("/slice", print_event)

	// watch for buffer setup complete
	print_buffer_ready := func(key string, value string) {
		fmt.Printf("Buffer ready %s with mountpoint %s", key, value)
	}
	go cli.WatchPrefix("/ready", print_buffer_ready)

	// add some fake buffers to test the watch
	ids := []int{1, 2, 3, 4, 5}
	for _, id := range ids {
		atomicAddBuffer(id)
	}
	atomicAddBuffer(16)
}
