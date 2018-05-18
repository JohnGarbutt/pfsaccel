package main

import (
	"fmt"
	"log"
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/clientv3util"
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
	var tidyup = func() {
		fmt.Println(kvc.Delete(context.Background(), "/buffer", clientv3.WithPrefix()))
	}
	tidyup()
	defer tidyup()

	// watch for updates
	rch := cli.Watch(context.Background(), "/buffer", clientv3.WithPrefix())
	var watch_buffer = func() {
		for wresp := range rch {
			for _, ev := range wresp.Events {
				fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}
	go watch_buffer()

	// atomic add new key
	var atomic_add = func(id int) {
		var  key = fmt.Sprintf("/buffer/%s", id)
		response, err := kvc.Txn(context.Background()).
			If(clientv3util.KeyMissing(key)).
			Then(clientv3.OpPut(key, "hello mr buffer")).
			Commit()
		if err != nil {
			panic(err)
		}
		if !response.Succeeded {
			panic("oh dear someone has added the key already")
		}
	}

	// add some fake buffers to test the watch
	ids := []int{1, 2, 3, 4, 5}
	for _, id := range ids {
		go atomic_add(id)
	}
	atomic_add(16)
}
