package registry

type Brick struct {
	Uuid string
	Name string
	CapacityGB uint
	Driver Driver
	AssignedBufferName string
	AssignedBufferIndex uint
}

type Driver int

const (
	Other Driver = iota
	Lustre
	BeeGFS
)