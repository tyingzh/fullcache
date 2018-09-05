package fullcache

//type SingleHeader struct {
//	Key string
//	Table string
//	Type CacheType
//}
//
type CacheType uint8

const (
	IDType CacheType = 0
	SingleType CacheType = 1
	MultiType CacheType = 2
)
