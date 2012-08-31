package dynamodb

import (
	"bytes"
	"encoding/json"
	"github.com/bmizerany/aws4"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

const iSO8601BasicFormat = "20060102T150405Z"

type Region struct {
	name     string
	endpoint string
}

func (r *Region) url() string {
	return "https://" + r.endpoint
}

var (
	USEast1      *Region = &Region{"us-east-1", "dynamodb.us-east-1.amazonaws.com"}
	USWest1      *Region = &Region{"us-west-1", "dynamodb.us-west-2.amazonaws.com"}
	EUWest1      *Region = &Region{"eu-west-1", "dynamodb.eu-west-1.amazonaws.com"}
	APNorthEast1 *Region = &Region{"ap-northeast-1", "dynamodb.ap-northeast-1.amazonaws.com"}
	APSouthEast1 *Region = &Region{"ap-southeast-1", "dynamodb.ap-southeast-1.amazonaws.com"}
)

type RequestError struct {
	Status  string
	Message string
}

func (r RequestError) Error() string {
	return "Status: " + r.Status + ", Message: " + r.Message
}

type Table struct {
	name    string
	region  *Region
	keys    *aws4.Keys
	service *aws4.Service
}

func NewTable(name string, region *Region, awsAccessKeyId string, awsSecretAccessKey string) *Table {
	k := &aws4.Keys{AccessKey: awsAccessKeyId, SecretKey: awsSecretAccessKey}
	s := &aws4.Service{Name: "dynamodb", Region: region.name}
	return &Table{name, region, k, s}
}

type PutRequestItem struct {
	Value interface{}
}

type PutItemRequest struct {
	TableName string
	Item      PutRequestItem
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

func (i *PutRequestItem) MarshalJSON() ([]byte, error) {
	var out bytes.Buffer

	v := reflect.ValueOf(i.Value)
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
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

func (t *Table) PutItem(item interface{}) error {
	data := PutItemRequest{TableName: t.name, Item: PutRequestItem{&item}}
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	log.Println(string(body))

	req, err := http.NewRequest("POST", t.region.url(), ioutil.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return err
	}

	req.ContentLength = int64(len(body))
	req.Header.Set("Host", t.region.endpoint)
	req.Header.Set("X-Amz-Target", "DynamoDB_20111205.PutItem")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format(iSO8601BasicFormat))
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("Connection", "Keep-Alive")

	err = t.service.Sign(t.keys, req)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return RequestError{Status: resp.Status, Message: string(body)}
	}

	return nil
}
