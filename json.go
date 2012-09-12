package dynamodb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Key struct {
	HashKeyElement  Field
	RangeKeyElement Field `json:",omitempty"`
}

type Field interface {
	Type() string
	Value() interface{}
}

type Number struct {
	N interface{} `json:",string"`
}

func (n *Number) Type() string {
	return "N"
}

func (n *Number) Value() interface{} {
	return n.N
}

type String struct {
	S string
}

func (s *String) Type() string {
	return "S"
}

func (s *String) Value() interface{} {
	return s.S
}

type Byte struct {
	B []byte `json:",string"`
}

func (b *Byte) Type() string {
	return "B"
}

func (b *Byte) Value() interface{} {
	return b.B
}

func NewField(value interface{}) (Field, error) {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		return &String{S: value.(string)}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return &Number{N: value}, nil
	}

	// TODO: []byte

	return nil, &json.MarshalerError{Type: v.Type()}
}

// UpdateItem

type UpdateItemRequest struct {
	TableName        string
	Key              Key
	AttributeUpdates map[string]Attribute
	Expected         map[string]Attribute `json:",omitempty"`
	ReturnValues     string
}

type Attribute struct {
	Value Field
}

func valuesToAttributeMap(item map[string]interface{}) (map[string]Attribute, error) {
	attrs := make(map[string]Attribute, len(item))
	for n, v := range item {
		f, err := NewField(v)
		if err != nil {
			return nil, err
		}

		attrs[n] = Attribute{Value: f}
	}
	return attrs, nil
}

func attributeMapToValues(attrs map[string]Attribute) map[string]interface{} {
	item := make(map[string]interface{}, len(attrs))
	for n, a := range attrs {
		item[n] = a.Value.Value()
	}
	return item
}

// Query

type QueryRequest struct {
	TableName         string
	HashKeyValue      Field
	ConsistentRead    bool            `json:",omitempty"`
	ScanIndexForward  bool            `json:",omitempty"`
	RangeKeyCondition QueryAttributes `json:",omitempty"`
	Limit             int             `json:",omitempty"`
	ExclusiveStartKey Key             `json:",omitempty"`
	AttributesToGet   []string        `json:",omitempty"`
}

type QueryAttributes struct {
	AttributeValueList []Field
	ComparisonOperator string
}

type QueryResponse struct {
	Count                 int
	Items                 []QueryItem
	LastEvaluatedKey      Key
	ConsumedCapacityUnits int
}

type QueryItem struct {
	Item map[string]Field
}

func (qi *QueryItem) Map() map[string]interface{} {
	r := make(map[string]interface{}, len(qi.Item))
	for n, f := range qi.Item {
		r[n] = f.Value()
	}
	return r
}

func (q *QueryItem) UnmarshalJSON(data []byte) error {
	var items map[string]interface{}
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	fields, err := itemsToFields(items)
	if err != nil {
		return err
	}
	q.Item = fields
	return nil
}

func itemsToFields(item map[string]interface{}) (map[string]Field, error) {
	result := make(map[string]Field, len(item))
	for name, raw := range item {
		attr := raw.(map[string]interface{})

		var value Field
		if v, ok := attr["S"]; ok {
			value = &String{S: v.(string)}
		} else if v, ok := attr["N"]; ok {
			n, err := parseNumber(v.(string))
			if err != nil {
				return result, err
			}
			value = &Number{N: n}
		} else if v, ok := attr["B"]; ok {
			value = &Byte{B: []byte(v.(string))}
		} else {
			var first string
			for k, _ := range attr {
				first = k
				break
			}
			return result, &UnsupportedTypeError{TypeId: first}
		}

		result[name] = value
	}

	return result, nil
}

func parseNumber(value string) (number interface{}, err error) {
	if strings.Contains(value, ".") {
		number, err = strconv.ParseFloat(value, 64)
	} else {
		number, err = strconv.ParseInt(value, 10, 64)
	}
	return
}

type UnsupportedTypeError struct {
	TypeId string
}

func (err *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Dynamo type %s is currently unsupported", err.TypeId)
}
