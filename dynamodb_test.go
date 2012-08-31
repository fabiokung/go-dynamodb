package dynamodb

import (
	"encoding/json"
	"github.com/bmizerany/assert"
	"testing"
)

type PutRequestWithString struct {
	Text string
}

func TestPutRequestStringSerialization(t *testing.T) {
	value := &PutRequestWithString{"some text"}
	item := PutRequestItem{value}
	data, err := json.Marshal(&item)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "{\"Text\":{\"S\":\"some text\"}}", string(data))
}

type PutRequestWithInt struct {
	N1 int
	N2 int8
	N3 int16
	N4 int32
	N5 int64

	N6  uint
	N7  uint8
	N8  uint16
	N9  uint32
	N10 uint64
}

func TestPutRequestIntSerialization(t *testing.T) {
	value := &PutRequestWithInt{12, 34, 56, 78, 90, 11, 21, 31, 41, 51}
	item := PutRequestItem{value}
	data, err := json.Marshal(&item)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t,
		"{\"N1\":{\"N\":\"12\"},\"N2\":{\"N\":\"34\"},\"N3\":{\"N\":\"56\"},"+
			"\"N4\":{\"N\":\"78\"},\"N5\":{\"N\":\"90\"},\"N6\":{\"N\":\"11\"},"+
			"\"N7\":{\"N\":\"21\"},\"N8\":{\"N\":\"31\"},\"N9\":{\"N\":\"41\"},"+
			"\"N10\":{\"N\":\"51\"}}", string(data))
}

type PutRequestWithFloat struct {
	N1 float32
	N2 float64
}

func TestPutRequestFloatSerialization(t *testing.T) {
	value := &PutRequestWithFloat{123.4567, 987654321.123456789}
	item := PutRequestItem{value}
	data, err := json.Marshal(&item)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t,
		"{\"N1\":{\"N\":\"123.4567\"},\"N2\":{\"N\":\"987654321.1234568\"}}",
		string(data))
}

func TestPutRequestBinarySerialization(t *testing.T) {
	t.Error("pending")
}
