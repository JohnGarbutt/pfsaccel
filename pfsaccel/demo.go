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

type etcKeystore struct {
	*clientv3.Client
}

func New() (*etcKeystore) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		log.Fatal(err)
		fmt.Println("Oh dear failed to create client...")
		panic(err)
	}
	return &etcKeystore{cli}
}

func (client *etcKeystore) CleanPrefix(prefix string) {
	kvc := clientv3.NewKV(client.Client)
	fmt.Println(kvc.Get(context.Background(), prefix, clientv3.WithPrefix()))
	kvc.Delete(context.Background(), prefix, clientv3.WithPrefix())
}


func (client *etcKeystore) AtomicAdd(key string, value string) {
	kvc := clientv3.NewKV(client.Client)
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

func (client *etcKeystore) WatchPrefix(prefix string, onPut func(event *clientv3.Event)) {
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

type keystore interface {
	Close() error
	CleanPrefix(prefix string)
	AtomicAdd(key string, value string)
	WatchPrefix(prefix string, onPut func(event *clientv3.Event))
}

func clearAllData(client keystore) {
	fmt.Println("Cleanup started")
	client.CleanPrefix("/buffer")
	client.CleanPrefix("/slice")
	client.CleanPrefix("/ready")
	fmt.Println("Cleanup done")
}

func main() {
	fmt.Println("Hello from pfsaccel demo.")

	var cli keystore =  New()
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
	make_slice := func(event *clientv3.Event) {
		cli.AtomicAdd(fmt.Sprintf("/slice/%d", slice_list_next.Value), string(event.Kv.Key))
		slice_list_next = slice_list_next.Next()
	}
	go cli.WatchPrefix("/buffer", make_slice)

	// watch for slice updates
	print_event := func(event *clientv3.Event) {
		buffer_key := event.Kv.Value
		fakeMountpoint, err := exec.Command("date", "-u", "-Ins").Output()
		if err != nil{
			panic(err)
		}
		cli.AtomicAdd(fmt.Sprintf("/ready%s", buffer_key), string(fakeMountpoint))
	}
	go cli.WatchPrefix("/slice", print_event)

	// watch for buffer setup complete
	print_buffer_ready := func(event *clientv3.Event) {
		fmt.Printf("Buffer ready %s with mountpoint %s", event.Kv.Key, event.Kv.Value)
	}
	go cli.WatchPrefix("/ready", print_buffer_ready)

	// add some fake buffers to test the watch
	ids := []int{1, 2, 3, 4, 5}
	for _, id := range ids {
		atomicAddBuffer(id)
	}
	atomicAddBuffer(16)
}
