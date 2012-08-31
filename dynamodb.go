package dynamodb

import (
	"bytes"
	"encoding/json"
	"github.com/bmizerany/aws4"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
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

type PutItemRequest struct {
	TableName string
	Item interface{}
}

func (t *Table) PutItem(item interface{}) (resp *http.Response, err error) {
	data := PutItemRequest{TableName: t.name, Item: item}
	body, err := json.Marshal(data)
	if err != nil {
		return
	}
	log.Println(string(body))

	req, err := http.NewRequest("POST", t.region.url(), ioutil.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return
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
		return
	}

	dump, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		return
	}
	log.Println("\n", string(dump))

	resp, err = http.DefaultClient.Do(req)

	return
}
