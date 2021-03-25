package codec

import (
	"encoding/json"

	"github.com/asim/go-micro/v3/broker"
)

// Marshaler is a simple encoding interface used for the broker/transport
// where headers are not supported by the underlying implementation.
type Marshaler struct{}

// Marshal returns the JSON encoding of v
func (Marshaler) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
func (Marshaler) Unmarshal(d []byte, v interface{}) error {
	if msg, ok := v.(*broker.Message); ok {
		if msg.Header == nil {
			msg.Header = make(map[string]string)
		}
		// used to handle message not from go-micro, which cannot be unmarshalled to broker.Message
		msg.Header["Content-Type"] = "application/json"
		msg.Body = d
	}
	// if d is published by go-micro, overwrite by unmarshal
	return json.Unmarshal(d, v)
}

func (Marshaler) String() string {
	return "json"
}
