package dynamodb

import (
	"bytes"
	"encoding/json"
	"github.com/bmizerany/aws4"
	"log"
	"net/http"
	"net/http/httputil"
	"time"
)

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

func (t *Table) PutItem(item interface{}) (resp *http.Response, err error) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)

	data := map[string]interface{}{
		"TableName": t.name,
		"Item":         item,
	}
	err = encoder.Encode(data)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", t.region.url(), buffer)
	if err != nil {
		return
	}

	date := time.Now().UTC().Format(http.TimeFormat)
	req.ContentLength = 0
	req.Header.Add("Host", t.region.endpoint)
	req.Header.Add("x-amz-target", "DynamoDB_20111205.PutItem")
	req.Header.Add("x-amz-date", date)
	req.Header.Add("Date", date)
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")
	req.Header.Add("Connection", "Keep-Alive")

	err = t.service.Sign(t.keys, req)
	if err != nil {
		return
	}

	resp, err = http.DefaultClient.Do(req)

	dump, err := httputil.DumpRequest(req, true)
	if err != nil {
		return
	}
	log.Println("\n", string(dump))

	return
}
