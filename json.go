package dynamodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type queryRequest struct {
	TableName      string
	Limit          int  `json:",omitempty"`
	ConsistentRead bool `json:",omitempty"`
	HashKeyValue   map[string]string
}

func (t *Table) queryRequestBody(key interface{}, limit int, consistent bool) ([]byte, error) {
	v := reflect.ValueOf(key)
	typeId, value, err := fieldToDynamoString(v)
	if err != nil {
		return []byte(""), err
	}

	request := &queryRequest{
		TableName:      t.name,
		Limit:          limit,
		ConsistentRead: consistent,
		HashKeyValue:   make(map[string]string)}
	request.HashKeyValue[typeId] = value
	return json.Marshal(request)
}

type putItemRequest struct {
	TableName string
	Item      putRequestItem
}

func (t *Table) putItemRequestBody(item interface{}) ([]byte, error) {
	data := putItemRequest{TableName: t.name, Item: putRequestItem{&item}}
	return json.Marshal(data)
}

type putRequestItem struct {
	Value interface{}
}

func (i putRequestItem) MarshalJSON() ([]byte, error) {
	var out bytes.Buffer

	v := reflect.ValueOf(i.Value)
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t := v.Type()

	out.WriteString("{")
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		out.WriteString("\"" + t.Field(i).Name + "\":")

		typeId, fieldVal, err := fieldToDynamoString(f)
		if err != nil {
			return []byte(""), err
		}

		out.WriteString("{")
		out.WriteString("\"" + typeId + "\":")
		out.WriteString("\"" + fieldVal + "\"")
		out.WriteString("}")

		if i < v.NumField()-1 {
			out.WriteString(",")
		}
	}
	out.WriteString("}")

	return out.Bytes(), nil
}

func dynamoItemToMap(item map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(item))
	for name, raw := range item {
		attr := raw.(map[string]interface{})

		var value interface{}
		if v, ok := attr["S"]; ok {
			value = v.(string)
		} else if v, ok := attr["N"]; ok {
			var err error
			value, err = parseNumber(v.(string))
			if err != nil {
				return result, err
			}
		} else if v, ok := attr["B"]; ok {
			value = []byte(v.(string))
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

func fieldToDynamoString(v reflect.Value) (typeId string, value string, err error) {
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {

	case reflect.String:
		return "S", v.Interface().(string), nil

	case reflect.Int:
		return "N", strconv.FormatInt(int64(v.Interface().(int)), 10), nil
	case reflect.Int8:
		return "N", strconv.FormatInt(int64(v.Interface().(int8)), 10), nil
	case reflect.Int16:
		return "N", strconv.FormatInt(int64(v.Interface().(int16)), 10), nil
	case reflect.Int32:
		return "N", strconv.FormatInt(int64(v.Interface().(int32)), 10), nil
	case reflect.Int64:
		return "N", strconv.FormatInt(v.Interface().(int64), 10), nil

	case reflect.Uint:
		return "N", strconv.FormatUint(uint64(v.Interface().(uint)), 10), nil
	case reflect.Uint8:
		return "N", strconv.FormatUint(uint64(v.Interface().(uint8)), 10), nil
	case reflect.Uint16:
		return "N", strconv.FormatUint(uint64(v.Interface().(uint16)), 10), nil
	case reflect.Uint32:
		return "N", strconv.FormatUint(uint64(v.Interface().(uint32)), 10), nil
	case reflect.Uint64:
		return "N", strconv.FormatUint(v.Interface().(uint64), 10), nil

	case reflect.Float32:
		return "N", strconv.FormatFloat(float64(v.Interface().(float32)), 'f', -1, 32), nil
	case reflect.Float64:
		return "N", strconv.FormatFloat(v.Interface().(float64), 'f', -1, 64), nil

	}

	return "", "", &json.MarshalerError{Type: v.Type()}
}

type UnsupportedTypeError struct {
	TypeId string
}

func (err *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Dynamo type %s is currently unsupported", err.TypeId)
}
