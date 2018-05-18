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
	}
	defer cli.Close()
	kvc := clientv3.NewKV(cli)

	// perform a put only if key is missing
	// It is useful to do the check atomically to avoid overwriting
	// the existing key which would generate potentially unwanted events,
	// unless of course you wanted to do an overwrite no matter what.
	response, err := kvc.Txn(context.Background()).
		If(clientv3util.KeyMissing("purpleidea1")).
		Then(clientv3.OpPut("purpleidea1", "hello world33")).
		Commit()
	if err != nil {
		log.Fatal(err)
	}
	if !response.Succeeded {
		panic("oh dear")
	}

	fmt.Println(kvc.Get(context.Background(), "purpleidea", clientv3.WithPrefix()))
}
