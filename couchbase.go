package couchbase

import (
	"fmt"
	"sync"
	"time"
	"errors"

	"github.com/couchbase/gocb"
	"github.com/twinj/uuid"

	. "github.com/Tlantic/mrs-integration-domain/storage"
)

//noinspection GoUnusedGlobalVariable
var (
	errInvalidQueryType = errors.New("Unsupported query type")
)

var mu = sync.Mutex{}
var clusters = map[string]*gocb.Cluster{}

// Assert interface implementation
var _ Database = (*CouchbaseStore)(nil)

type CouchbaseStore struct {
	name   string
	bucket *gocb.Bucket
}

//noinspection ALL
func NewCouchbaseStore(host, bucketName, bucketPassword string) (*CouchbaseStore, error) {
	defer mu.Unlock()
	mu.Lock()

	var err error
	var clust *gocb.Cluster

	if clust = clusters[host]; clust == nil {
		if clust, err = gocb.Connect(host); err != nil {
			return nil, err
		}
		clusters[host] = clust
	}

	if b, err := clust.OpenBucket(bucketName, bucketPassword); err == nil {
		return &CouchbaseStore{name: "couchbase", bucket: b }, nil
	} else {
		return nil, err
	}
}

func (c *CouchbaseStore) Close() {
	if c.bucket != nil {
		c.bucket.Close()
	}
}

func (c *CouchbaseStore) NewRow(id string) Row {
	return newDoc(id)
}
func (c *CouchbaseStore) NewQuery(statement string) Query {
	return newQuery(statement)
}

func (c *CouchbaseStore) GetName() string {
	return c.name
}
func (c *CouchbaseStore) SetName(name string) error {
	c.name = name
	return nil
}

func (c *CouchbaseStore) Create(xs ...interface{}) ([]Row, bool) {

	isOk := true
	length := len(xs)
	rows := make([]Row, length, length)
	bulkOps := make([]gocb.BulkOp, length, length)
	now := time.Now().UTC()

	for i := 0; i < length; i++ {
		doc := newDoc("")
		rows[i] = doc
		doc.Meta[CREATEDON] = now
		doc.Meta[UPDATEDON] = now

		switch value := xs[i].(type) {
		case string:
			bulkOps[i] = &gocb.InsertOp{
				Key:   value,
				Value: doc,
			}
		case Row:

			var expiry uint32
			var cas gocb.Cas

			doc.key = value.GetKey()
			doc.Id = value.GetId()
			doc.Type = value.GetType()
			doc.Data = value.GetData()
			doc.mergeMetadata(value.Metadata())

			if value, ok := value.GetMeta(TTL).(uint32); ok {
				expiry = value
			}

			if value, ok := value.GetMeta(CAS).(gocb.Cas); ok {
				cas = value
			}

			bulkOps[i] = &gocb.InsertOp{
				Key:    doc.GetKey(),
				Value:  doc,
				Expiry: expiry,
				Cas:    cas,
			}
		case fmt.Stringer:
			bulkOps[i] = &gocb.InsertOp{
				Key:   value.String(),
				Value: doc,
			}
		default:
			doc.fault = errors.New("Unsupported type, expecting string, Stringer or Row.")
			isOk = false
		}
	}

	if c.bucket.Do(bulkOps) != nil {
		isOk = false
	}
	for i := 0; i < length; i++ {
		op := bulkOps[i].(*gocb.InsertOp)
		doc := rows[i].(*doc)
		doc.Meta[CAS] = op.Cas
		doc.Meta[EXPIRY] = op.Expiry
		if op.Err != nil {
			isOk = false
			doc.fault = op.Err
		}
	}

	return rows, isOk
}
func (c *CouchbaseStore) CreateOne(x interface{}) Row {

	var expiry uint32
	doc := newDoc("")

	now := time.Now().UTC()
	doc.Meta[CREATEDON] = now
	doc.Meta[UPDATEDON] = now

	if row, ok := x.(Row); ok {

		doc.key = row.GetKey()
		doc.Id = row.GetId()
		doc.Type = row.GetType()
		doc.Data = row.GetData()
		doc.mergeMetadata(row.Metadata())

		if value, ok := row.GetMeta(TTL).(uint32); ok {
			expiry = value
			doc.Meta[TTL] = value
		}
	} else {
		doc.Id = uuid.NewV4().String()
		doc.Data = x
	}

	if cas, err := c.bucket.Insert(doc.GetKey(), doc, expiry); err != nil {
		doc.fault = err
	} else {
		doc.Meta[CAS] = cas
	}
	return doc
}

func (c *CouchbaseStore) Read(xs ...interface{}) ([]Row, bool) {

	isOk := true
	length := len(xs)
	rows := make([]Row, length, length)
	bulkOps := make([]gocb.BulkOp, length, length)

	for i := 0; i < length; i++ {

		doc := newDoc("")
		rows[i] = doc

		switch value := xs[i].(type) {
		case string:
			bulkOps[i] = &gocb.GetOp{
				Key:   value,
				Value: doc,
			}
		case Row:
			doc.key = value.GetKey()
			doc.Id = value.GetId()
			doc.Type = value.GetType()
			doc.Data = value.GetData()
			doc.mergeMetadata(value.Metadata())

			cas, _ := value.GetMeta(CAS).(gocb.Cas)

			bulkOps[i] = &gocb.GetOp{
				Key:   doc.GetKey(),
				Cas:   cas,
				Value: doc,
			}
		case fmt.Stringer:
			bulkOps[i] = &gocb.GetOp{
				Key:   value.String(),
				Value: doc,
			}
		default:
			doc.fault = errors.New("Unsupported type, expecting string, Stringer or Row.")
			isOk = false
		}
	}

	if c.bucket.Do(bulkOps) != nil {
		isOk = false
	}
	for i := 0; i < length; i++ {
		op := bulkOps[i].(*gocb.GetOp)
		doc := rows[i].(*doc)
		doc.Meta[CAS] = op.Cas
		if op.Err != nil {
			isOk = false
			doc.fault = op.Err
		}
	}

	return rows, isOk
}
func (c *CouchbaseStore) ReadOne(x interface{}) Row {
	return c.ReadOneWithType(x, nil)
}
func (c *CouchbaseStore) ReadOneWithType(x interface{}, out interface{}) Row {

	doc := newDoc("")
	doc.Data = out

	switch value := x.(type) {
	case string:
		doc.key = value
	case Row:

		doc.key = value.GetKey()
		doc.Id = value.GetId()
		doc.Type = value.GetType()
		if doc.Data == nil {
			doc.Data = value.GetData()
		}
		doc.mergeMetadata(value.Metadata())
	case fmt.Stringer:
		doc.key = value.String()
	}



	if lock, ok := doc.GetMeta(LOCK).(uint32); ok && lock > 0 {
		var locktime uint32
		locktime, _ = doc.GetMeta(TIMEOUT).(uint32)
		if cas, err := c.bucket.GetAndLock(doc.GetKey(), locktime, doc); err != nil {
			doc.fault = err
		} else {
			doc.Meta[CAS] = cas
		}
	} else if cas, err := c.bucket.Get(doc.GetKey(), doc); err != nil {
		doc.fault = err
	} else {
		doc.Meta[CAS] = cas
	}

	return doc
}

func (c *CouchbaseStore) Replace(xs ...interface{}) ([]Row, bool) {

	isOk := true
	length := len(xs)
	rows := make([]Row, length, length)
	bulkOps := make([]gocb.BulkOp, length, length)
	now := time.Now().UTC()

	for i := 0; i < length; i++ {
		doc := newDoc("")
		rows[i] = doc

		doc.Meta[UPDATEDON] = now

		if value, ok := xs[i].(Row); ok {

			doc.key = value.GetKey()
			doc.Id = value.GetId()
			doc.Type = value.GetType()
			doc.Data = value.GetData()

			cas, _ := value.GetMeta(CAS).(gocb.Cas)

			expiry, _ := value.GetMeta(TTL).(uint32)

			bulkOps[i] = &gocb.ReplaceOp{
				Key:    doc.GetKey(),
				Cas:    cas,
				Value:  doc,
				Expiry: expiry,
			}

		} else {
			doc.fault = errors.New("Unsupported type, expecting Row.")
			isOk = false
		}
	}

	if c.bucket.Do(bulkOps) != nil {
		isOk = false
	}
	for i := 0; i < length; i++ {

		op := bulkOps[i].(*gocb.ReplaceOp)
		doc := rows[i].(*doc)

		doc.Meta[CAS] = op.Cas
		doc.Meta[TTL] = op.Expiry

		if op.Err != nil {
			isOk = false
			doc.fault = op.Err
		}
	}

	return rows, isOk
}
func (c *CouchbaseStore) ReplaceOne(x interface{}) Row {

	doc := newDoc("")
	var expiry uint32
	var cas gocb.Cas

	now := time.Now().UTC()
	doc.Meta[UPDATEDON] = now

	if value, ok := x.(Row); ok {

		doc.key = value.GetKey()
		doc.Id = value.GetId()
		doc.Type = value.GetType()
		doc.Data = value.GetData()
		doc.mergeMetadata(value.Metadata())

		if value, ok := value.GetMeta(TTL).(uint32); ok {
			expiry = value
			doc.Meta[TTL] = value
		}
		if value, ok := value.GetMeta(CAS).(gocb.Cas); ok {
			cas = value
		}
	} else {
		doc.Id = uuid.NewV4().String()
		doc.Data = x
	}

	if cas, err := c.bucket.Replace(doc.GetKey(), doc, cas, expiry); err != nil {
		doc.fault = err
	} else {
		doc.Meta[CAS] = cas
	}

	return doc
}

func (c *CouchbaseStore) Update(xs ...interface{}) ([]Row, bool) {
	var faulted bool

	var length int = len(xs)
	var rows = make([]Row, length, length)

	for i := 0; i < length; i++ {
		rows[i] = c.UpdateOne(xs[i])
		faulted = rows[i].IsFaulted()
	}
	return rows, faulted
}
func (c *CouchbaseStore) UpdateOne(x interface{}) Row {

	return &doc{
		fault: errors.New("Not Implemented"),
	}
}

func (c *CouchbaseStore) Destroy(xs ...interface{}) ([]Row, bool) {
	var faulted bool

	var length int = len(xs)
	var rows = make([]Row, length, length)

	for i := 0; i < length; i++ {
		rows[i] = c.DestroyOne(xs[i])
		faulted = rows[i].IsFaulted()
	}
	return rows, faulted
}
func (c *CouchbaseStore) DestroyOne(x interface{}) Row {

	var cas gocb.Cas
	doc := newDoc("")

	switch value := x.(type) {
	case string:
		doc.key = value
	case Row:

		doc.key = value.GetKey()
		doc.Id = value.GetId()
		doc.Type = value.GetType()
		doc.Data = value.GetData()
		doc.mergeMetadata(value.Metadata())
		cas, _ = value.GetMeta(CAS).(gocb.Cas)
	case fmt.Stringer:
		doc.key = value.String()
	}

	if cas, err := c.bucket.Remove(doc.GetKey(), cas); err != nil {
		doc.fault = err
	} else {
		doc.Meta[CAS] = cas
	}

	return doc
}

func (c *CouchbaseStore) Exec(q Query) (QueryResult, error) {
	params := q.GetParams()
	n1qlquery := gocb.NewN1qlQuery(q.GetStatement())

	if value, ok := q.GetMeta(ADHOC).(bool); ok {
		n1qlquery.AdHoc(value)
	}

	if value, ok := q.GetMeta(CONSISTENCY).(int); ok {
		n1qlquery.Consistency(gocb.ConsistencyMode(value))
	}

	if value, ok := q.GetMeta(TIMEOUT).(time.Duration); ok {
		n1qlquery.Timeout(value)
	}

	if results, err := c.bucket.ExecuteN1qlQuery(n1qlquery, params); err != nil {
		return nil, err
	} else {
		return newQueryResult(q, results), nil
	}

}
