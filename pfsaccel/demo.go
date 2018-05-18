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

func main() {
	fmt.Println("Hello from pfsaccel demo.")

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		log.Fatal(err)
		fmt.Println("hello")
	}
	defer cli.Close()
	kvc := clientv3.NewKV(cli)

	// tidy up keys once we are finished
	clean_prefx := func(prefix string) {
		fmt.Println(kvc.Get(context.Background(), prefix, clientv3.WithPrefix()))
		fmt.Println(kvc.Delete(context.Background(), prefix, clientv3.WithPrefix()))
	}
	var tidyup = func() {
		clean_prefx("/buffer")
		clean_prefx("/slice")
		clean_prefx("/ready")
	}
	tidyup()
	defer tidyup()

	var atomic_add = func(key string, value string) {
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

	var atomic_add_buffer = func(id int) {
		var  key = fmt.Sprintf("/buffer/%d", id)
		atomic_add(key, "I am a new buffer")
	}

	// list of "available" slice ids
	slice_list := list.New()
	slice_ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, id := range slice_ids {
		slice_list.PushBack(id)
	}
	slice_list_next := slice_list.Front()

	var watch_prefix = func(prefix string, onPut func(event *clientv3.Event)) {
		rch := cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
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

	// watch for buffers, create slice on put
	make_slice := func(event *clientv3.Event) {
		atomic_add(fmt.Sprintf("/slice/%d", slice_list_next.Value), string(event.Kv.Key))
		slice_list_next = slice_list_next.Next()
	}
	go watch_prefix("/buffer", make_slice)

	// watch for slice updates
	print_event := func(event *clientv3.Event) {
		buffer_key := event.Kv.Value
		fakeMountpoint, err := exec.Command("date", "-u", "-Ins").Output()
		if err != nil{
			panic(err)
		}
		atomic_add(fmt.Sprintf("/ready%s", buffer_key), string(fakeMountpoint))
	}
	go watch_prefix("/slice", print_event)

	// watch for buffer setup complete
	print_buffer_ready := func(event *clientv3.Event) {
		fmt.Printf("Buffer ready %s with mountpoint %s", event.Kv.Key, event.Kv.Value)
	}
	go watch_prefix("/ready", print_buffer_ready)

	// add some fake buffers to test the watch
	ids := []int{1, 2, 3, 4, 5}
	for _, id := range ids {
		atomic_add_buffer(id)
	}
	atomic_add_buffer(16)
}
