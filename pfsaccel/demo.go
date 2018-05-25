package main

import (
	"fmt"
	"log"
	"context"
	"container/list"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/clientv3util"
	"os/exec"
)

// TODO: add and interface here
func createStorageClient() (*clientv3.Client) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		log.Fatal(err)
		fmt.Println("Oh dear failed to create client...")
		panic(err)
	}
	return cli
}

func cleanPrefix(client *clientv3.Client, prefix string) {
	kvc := clientv3.NewKV(client)
	fmt.Println(kvc.Get(context.Background(), prefix, clientv3.WithPrefix()))
	kvc.Delete(context.Background(), prefix, clientv3.WithPrefix())
}

func clearAllData(client *clientv3.Client) {
	fmt.Println("Cleanup started")
	cleanPrefix(client, "/buffer")
	cleanPrefix(client, "/slice")
	cleanPrefix(client, "/ready")
	fmt.Println("Cleanup done")
}

func atomicAdd(client *clientv3.Client, key string, value string) {
	kvc := clientv3.NewKV(client)
	response, err := kvc.Txn(context.Background()).
		If(clientv3util.KeyMissing(key)).
		Then(clientv3.OpPut(key, value)).
		Commit()
	if err != nil {
		panic(err)
	}
	if !response.Succeeded {
		panic("oh dear someone has added the key already")
	}
}

func watchPrefix(client *clientv3.Client, prefix string, onPut func(event *clientv3.Event)) {
	rch := client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			if ev.Type.String() == "PUT" {
				onPut(ev)
			} else {
				fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}
}

func main() {
	fmt.Println("Hello from pfsaccel demo.")

	cli := createStorageClient()
	defer cli.Close()

	// tidy up keys before we start and after we are finished
	clearAllData(cli)
	defer clearAllData(cli)

	var atomic_add_buffer = func(id int) {
		var  key = fmt.Sprintf("/buffer/%d", id)
		atomicAdd(cli, key, "I am a new buffer")
	}

	// list of "available" slice ids
	slice_list := list.New()
	slice_ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, id := range slice_ids {
		slice_list.PushBack(id)
	}
	slice_list_next := slice_list.Front()

	// watch for buffers, create slice on put
	make_slice := func(event *clientv3.Event) {
		atomicAdd(cli, fmt.Sprintf("/slice/%d", slice_list_next.Value), string(event.Kv.Key))
		slice_list_next = slice_list_next.Next()
	}
	go watchPrefix(cli, "/buffer", make_slice)

	// watch for slice updates
	print_event := func(event *clientv3.Event) {
		buffer_key := event.Kv.Value
		fakeMountpoint, err := exec.Command("date", "-u", "-Ins").Output()
		if err != nil{
			panic(err)
		}
		atomicAdd(cli, fmt.Sprintf("/ready%s", buffer_key), string(fakeMountpoint))
	}
	go watchPrefix(cli, "/slice", print_event)

	// watch for buffer setup complete
	print_buffer_ready := func(event *clientv3.Event) {
		fmt.Printf("Buffer ready %s with mountpoint %s", event.Kv.Key, event.Kv.Value)
	}
	go watchPrefix(cli, "/ready", print_buffer_ready)

	// add some fake buffers to test the watch
	ids := []int{1, 2, 3, 4, 5}
	for _, id := range ids {
		atomic_add_buffer(id)
	}
	atomic_add_buffer(16)
}
