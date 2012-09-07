package dynamodb

import (
	"bytes"
	"encoding/json"
	"github.com/bmizerany/aws4"
	"io/ioutil"
	"net/http"
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

func (t *Table) PutItem(item interface{}) error {
	body, err := t.putItemRequestBody(item)
	if err != nil {
		return err
	}

	_, err = t.doDynamoRequest("PutItem", body)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) Query(key interface{}, limit int, consistent bool) ([]map[string]interface{}, error) {
	body, err := t.queryRequestBody(key, limit, consistent)
	if err != nil {
		return nil, err
	}

	resp, err := t.doDynamoRequest("Query", body)
	if err != nil {
		return nil, err
	}

	data := make(map[string]interface{})
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}

	items := data["Items"].([]interface{})
	parsed := make([]map[string]interface{}, len(items))
	for i, raw := range items {
		item := raw.(map[string]interface{})
		parsed[i], err = dynamoItemToMap(item)
		if err != nil {
			return parsed, err
		}
	}

	return parsed, nil
}

func (t *Table) doDynamoRequest(operation string, body []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", t.region.url(), ioutil.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return nil, err
	}

	req.ContentLength = int64(len(body))
	req.Header.Set("Host", t.region.endpoint)
	req.Header.Set("X-Amz-Target", "DynamoDB_20111205."+operation)
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("Connection", "Keep-Alive")

	err = t.service.Sign(t.keys, req)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return body, RequestError{Status: resp.Status, Message: string(body)}
	}

	return body, err
}

type RequestError struct {
	Status  string
	Message string
}

func (r RequestError) Error() string {
	return "Status: " + r.Status + ", Message: " + r.Message
}
