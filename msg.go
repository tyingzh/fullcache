package fullcache


// TODO other caches subscribe this listener
type Msg struct {
	Type MsgType
	Data string
}

type MsgType uint8
const (
	MsgUpdateRow  MsgType = 0
	MsgAlterTable MsgType = 1
	MsgDeleteRow  MsgType = 2
)


type Empty struct {}
