package fullcache

type ICache interface{
	Get(key string) (string, error)
	Set(key, value string) error
	Del(key string) error
	OnMiss(key string) (value string, err error)
}
