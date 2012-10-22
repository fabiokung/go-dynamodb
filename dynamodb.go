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

type Region struct {
	name     string
	endpoint string
}

func (r *Region) url() string {
	return "https://" + r.endpoint
}

var (
	USEast1      *Region = &Region{"us-east-1", "dynamodb.us-east-1.amazonaws.com"}
	USWest1      *Region = &Region{"us-west-1", "dynamodb.us-west-1.amazonaws.com"}
	USWest2      *Region = &Region{"us-west-2", "dynamodb.us-west-2.amazonaws.com"}
	EUWest1      *Region = &Region{"eu-west-1", "dynamodb.eu-west-1.amazonaws.com"}
	APNorthEast1 *Region = &Region{"ap-northeast-1", "dynamodb.ap-northeast-1.amazonaws.com"}
	APSouthEast1 *Region = &Region{"ap-southeast-1", "dynamodb.ap-southeast-1.amazonaws.com"}
)

type Table struct {
	name      string
	region    *Region
	keys      *aws4.Keys
	service   *aws4.Service
	debugMode bool // causes requests to be dumped
}

func NewTable(name string, region *Region, awsAccessKeyId string,
	awsSecretAccessKey string, debugMode bool) *Table {

	k := &aws4.Keys{AccessKey: awsAccessKeyId, SecretKey: awsSecretAccessKey}
	s := &aws4.Service{Name: "dynamodb", Region: region.name}
	return &Table{name, region, k, s, debugMode}
}

func (t *Table) DeleteItem(key interface{}) (map[string]interface{}, float64, error) {
	k, err := NewField(key)
	if err != nil {
		return nil, 0, err
	}

	r := new(DeleteItemRequest)
	r.TableName = t.name
	r.Key = Key{HashKeyElement: k}
	r.ReturnValues = "ALL_OLD"

	rawResp, err := t.doDynamoRequest("DeleteItem", r)
	if err != nil {
		return nil, 0, err
	}

	type DeleteItemResponse struct {
		Attributes            map[string]Field
		ConsumedCapacityUnits float64
	}

	resp := new(DeleteItemResponse)
	err = json.Unmarshal(rawResp, resp)
	if err != nil {
		return nil, 0, err
	}
	return fieldMapToValues(resp.Attributes), resp.ConsumedCapacityUnits, nil
}

func (t *Table) UpdateItem(key interface{}, item map[string]interface{}) (map[string]interface{}, float64, error) {
	k, err := NewField(key)
	if err != nil {
		return nil, 0, err
	}
	attrs, err := valuesToAttributeMap(item)
	if err != nil {
		return nil, 0, err
	}

	r := new(UpdateItemRequest)
	r.TableName = t.name
	r.Key = Key{HashKeyElement: k}
	r.AttributeUpdates = attrs
	r.ReturnValues = "UPDATED_OLD"

	rawResp, err := t.doDynamoRequest("UpdateItem", r)
	if err != nil {
		return nil, 0, err
	}

	type UpdateItemResponse struct {
		Attributes            map[string]Field
		ConsumedCapacityUnits float64
	}

	resp := new(UpdateItemResponse)
	err = json.Unmarshal(rawResp, resp)
	if err != nil {
		return nil, 0, err
	}
	return fieldMapToValues(resp.Attributes), resp.ConsumedCapacityUnits, nil
}

func (t *Table) Query(key interface{}, consistent bool) ([]map[string]interface{}, error) {
	k, err := NewField(key)
	if err != nil {
		return nil, err
	}

	r := new(QueryRequest)
	r.TableName = t.name
	r.HashKeyValue = k
	r.ConsistentRead = consistent

	rawResp, err := t.doDynamoRequest("Query", r)
	if err != nil {
		return nil, err
	}
	resp := new(QueryResponse)
	err = json.Unmarshal(rawResp, &resp)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]interface{}, len(resp.Items))
	for i, item := range resp.Items {
		items[i] = item.Map()
	}
	return items, nil
}

func (t *Table) GetItem(key interface{}, consistent bool) (map[string]interface{}, error) {
	k, err := NewField(key)
	if err != nil {
		return nil, err
	}

	r := new(GetItemRequest)
	r.TableName = t.name
	r.Key = Key{HashKeyElement: k}
	r.ConsistentRead = consistent

	rawResp, err := t.doDynamoRequest("GetItem", r)
	if err != nil {
		return nil, err
	}
	resp := new(GetItemResponse)
	err = json.Unmarshal(rawResp, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Item.Map(), nil
}

func (t *Table) doDynamoRequest(operation string, body interface{}) ([]byte, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(body); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", t.region.url(), &b)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("X-Amz-Target", "DynamoDB_20111205."+operation)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("Connection", "Keep-Alive")

	err = t.service.Sign(t.keys, req)
	if err != nil {
		return nil, err
	}

	if t.debugMode {
		out, err := httputil.DumpRequestOut(req, true)
		if err != nil {
			return nil, err
		}
		log.Println(string(out))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return respBody, RequestError{Status: resp.Status, Message: string(respBody)}
	}

	return respBody, err
}

type RequestError struct {
	Status  string
	Message string
}

func (r RequestError) Error() string {
	return "Status: " + r.Status + ", Message: " + r.Message
}
