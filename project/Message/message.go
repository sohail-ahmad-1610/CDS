package message

// -----------------------------------------------------------------------------

// Message Type.
const (
	REGISTER = iota
	OK
	START_TASK
	HB // HB = heart beat
	ABORT
)

// -----------------------------------------------------------------------------

// Slave Status.
const (
	ALIVE = iota
	KILL
	BUSY
	IDOL // HB = heart beat
)

// -----------------------------------------------------------------------------

// Message is a `struct` used for communication between namenode and datanode(workhorse/slave).
type Message struct {
	SlaveID int
	Content string
	Type    int
}
