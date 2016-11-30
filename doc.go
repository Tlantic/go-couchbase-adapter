package couchbase

import (
	"time"
	"bytes"
	"encoding/json"
	"github.com/Tlantic/mrs-integration-domain/storage"
	"strings"
	"reflect"
)

// Assert interface implementation
var _ storage.Row = (*doc)(nil)

type raw struct {
	Id   string                        `json:"_uId"`
	Type string                        `json:"_type"`
	Data json.RawMessage               `json:"data"`
	Meta map[string]interface{}        `json:"meta"`
}

type doc struct {
	key   string
	fault error

	Id    string                        `json:"_uId"`
	Type  string                        `json:"_type"`
	Data  interface{}                 `json:"data"`
	Meta  map[string]interface{}      `json:"meta"`
}
func (row *doc) mergeMetadata(src map[string]interface{})  {
 for k, v := range src {
	 if (row.Meta[k] == nil) {
		 row.Meta[k] = v
	 }
 }
}
//noinspection ALL
func newDoc(id string) *doc {
	return &doc{
		Id: id,
		Meta: map[string]interface{}{
			storage.TTL: new(uint32),
		},
	}
}


func (row *doc) GetKey() string {
	if row.key == "" {
		if row.Data != nil {
			buf := bytes.NewBufferString(row.GetType())
			buf.WriteString("::")
			buf.WriteString(row.Id)
			return buf.String()
		} else {
			return row.Id
		}
	}
	return row.key
}
func (row *doc) SetKey(value string) {
	row.key = value
}

func (row *doc) GetId() string {
	return row.Id
}
func (row *doc) SetId(value string) {
	row.Id = value
}

func (row *doc) GetType() string {
	if (row.Type == "" && row.Data != nil) {
		return strings.ToLower(reflect.TypeOf(row.Data).Elem().Name())
	}
	return row.Type
}
func (row *doc) SetType(value string) {
	row.Type = value
}

func (row *doc) SetData(data interface{}) {
	row.Data = data
}
func (row *doc) GetData() interface{} {
	return row.Data
}

func (doc *doc) SetMeta(key string, value interface{}) {
	doc.Meta[key] = value
}
func (doc *doc) GetMeta(key string) interface{} {
	return doc.Meta[key]
}
func (doc *doc) Metadata() map[string]interface{} {
	cpy := make(map[string]interface{})
	for k,v := range doc.Meta {
		cpy[k] = v
	}
	return cpy
}

func (doc *doc) CreatedOn() *time.Time {
	switch value := doc.Meta[storage.CREATEDON].(type) {
	case time.Time:
		return &value
	case int64:
		ts := time.Unix(0, value)
		return &ts
	default:
		return nil
	}
}
func (doc *doc) UpdatedOn() *time.Time {
	switch value := doc.Meta[storage.UPDATEDON].(type) {
	case time.Time:
		return &value
	case int64:
		ts := time.Unix(0, value)
		return &ts
	default:
		return nil
	}
}

func (doc *doc) SetExpiry(time uint32) {
	doc.Meta[storage.TTL] = time
}

func (doc *doc) IsFaulted() bool {
	return doc.fault != nil
}
func (doc *doc) Fault() error {
	return doc.fault
}

func (doc *doc) MarshalJSON() ([]byte, error) {
	pre := raw{
		Id: doc.GetId(),
		Type: doc.GetType(),
		Meta: doc.Meta,
	}

	switch value := pre.Meta[storage.CREATEDON].(type) {
	case time.Time:
		pre.Meta[storage.CREATEDON] = value.UnixNano()
	case int64:
		pre.Meta[storage.CREATEDON] = value
	default:
		delete(pre.Meta, storage.CREATEDON)
	}

	switch value := pre.Meta[storage.UPDATEDON].(type) {
	case time.Time:
		pre.Meta[storage.UPDATEDON] = value.UnixNano()
	case int64:
		pre.Meta[storage.UPDATEDON] = value
	default:
		delete(pre.Meta, storage.UPDATEDON)
	}

	if data, err := json.Marshal(doc.Data); err != nil {
		return nil, err
	} else {
		pre.Data = data
	}

	return json.Marshal(&pre)
}
func (doc *doc) UnmarshalJSON(data []byte) error {

	pre := raw{}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&pre); err != nil {
		return err
	}

	doc.Id = pre.Id
	doc.Type = pre.Type

	if dbytes, err := pre.Data.MarshalJSON(); err != nil {
		return err
	} else if doc.Data == nil {
		doc.Data = dbytes
	} else if err := json.Unmarshal(dbytes, doc.Data); err != nil {
		return err
	}

	doc.Meta = pre.Meta
	cdate := pre.Meta[storage.CREATEDON]
	if ( cdate != nil ) {
		switch value := cdate.(type) {
		case json.Number:
			if value, err := value.Int64(); err == nil {
				doc.Meta[storage.CREATEDON] = time.Unix(0, value)
			}
		}
	}

	udate := pre.Meta[storage.UPDATEDON]
	if ( udate != nil ) {
		switch value := udate.(type) {
		case json.Number:
			if value, err := value.Int64(); err == nil {
				doc.Meta[storage.UPDATEDON] = time.Unix(0, value)
			}
		}
	}

	return nil
}