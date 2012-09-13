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

type Int struct {
	N int64 `json:",string"`
}

func (n *Int) Type() string {
	return "N"
}

func (n *Int) Value() interface{} {
	return n.N
}

type Float struct {
	N float64 `json:",string"`
}

func (n *Float) Type() string {
	return "N"
}

func (n *Float) Value() interface{} {
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

	if !v.IsValid() {
		return nil, nil
	}

	switch v.Kind() {
	case reflect.String:
		return &String{S: value.(string)}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return &Int{N: value.(int64)}, nil
	case reflect.Float32, reflect.Float64:
		return &Float{N: value.(float64)}, nil
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
	Value  Field  `json:",omitempty"`
	Action string `json:",omitempty"`
}

func valuesToAttributeMap(item map[string]interface{}) (map[string]Attribute, error) {
	attrs := make(map[string]Attribute, len(item))
	for n, v := range item {
		if v == nil {
			attrs[n] = Attribute{Action: "DELETE"}
			continue
		}

		f, err := NewField(v)
		if err != nil {
			return attrs, err
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
	Items                 []Item
	LastEvaluatedKey      Key
	ConsumedCapacityUnits float64
}

// GetItem

type GetItemRequest struct {
	TableName       string
	Key             Key
	AttributesToGet []string `json:",omitempty"`
	ConsistentRead  bool     `json:",omitempty"`
}

type GetItemResponse struct {
	Item                  Item
	ConsumedCapacityUnits float64
}

type Item struct {
	Fields map[string]Field
}

func (qi *Item) Map() map[string]interface{} {
	r := make(map[string]interface{}, len(qi.Fields))
	for n, f := range qi.Fields {
		r[n] = f.Value()
	}
	return r
}

func (q *Item) UnmarshalJSON(data []byte) error {
	var items map[string]interface{}
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	fields, err := itemsToFields(items)
	if err != nil {
		return err
	}
	q.Fields = fields
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
			var err error
			value, err = parseNumber(v.(string))
			if err != nil {
				return result, err
			}
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

func parseNumber(value string) (Field, error) {
	if strings.Contains(value, ".") {
		n, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return &Float{N: n}, nil
	}

	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, err
	}
	return &Int{N: n}, nil
}

type UnsupportedTypeError struct {
	TypeId string
}

func (err *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Dynamo type %s is currently unsupported", err.TypeId)
}
