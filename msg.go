package fullcache


// TODO other caches subscribe this listener
type Msg struct {
	Type MsgType
	Data string
}

type MsgType uint8
const (
	MsgUpdateTable MsgType = 0
	MsgAlterTable MsgType = 1
)


type Empty struct {}
