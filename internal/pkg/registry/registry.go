package registry

type Registry interface {
	AllBuffers() []Buffer
	Buffer(name string) Buffer

	AllBricks() []Brick
	Bricks(host string) []Brick

	UpdateHost(host Host)
	AddBuffer(buffer Buffer) error
	AddBricksToBuffer(buffer Buffer, bricks []Brick) (Buffer, error)
}

type BrickFinder interface {
	GetBricksForBuffer(buffer Buffer, registry Registry)
}