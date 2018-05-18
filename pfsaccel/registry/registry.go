package registry

import "fmt"

type Owner string

type Buffer struct {
	token string
	slices int
	owner Owner
}

func GetBuffer(token string) Buffer {
	fmt.Println("Get buffer", token)
}