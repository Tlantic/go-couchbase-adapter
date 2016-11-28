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

//noinspection GoUnusedConst
const (
	CAS = "cas"
	ADHOC = "adhoc"
	CONSISTENCY = "consistency"
	CONSISTENTWITH = "consistent_with"
	TIMEOUT = "timeout"
)

var (
	errInvalidQueryType = errors.New("Unsupported query type")
)

var mu = sync.Mutex{}
var clusters = map[string]*gocb.Cluster{}


// Assert interface implementation
var _ Database = (*CouchbaseStore)(nil)



type CouchbaseStore struct {
	name     string
	bucket   *gocb.Bucket
}

//noinspection GoUnusedExportedFunction
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
	}
	return nil, err
}

func ( c *CouchbaseStore ) Close() {
	c.bucket.Close()
}

func (c *CouchbaseStore) GetName() string {
	return c.name
}
func (c *CouchbaseStore) SetName(name string) error {
	c.name = name
	return nil
}


func (c *CouchbaseStore) Create(xs ... interface{}) ([]Row, bool) {

	isOk := true
	length := len(xs)
	rows := make([]Row, length, length)
	bulkOps := make([]gocb.BulkOp, length, length)
	now := time.Now().UTC()

	for i:=0;i<length;i++ {
		doc := newDoc()
		rows[i] = doc
		doc.Meta[CREATEDON] = now
		doc.Meta[UPDATEDON] = now

		switch value := xs[i].(type) {
		case string:
			bulkOps[i] = &gocb.InsertOp{
				Key: value,
				Value: doc,
			}
		case Row:

			var expiry uint32
			var cas gocb.Cas

			doc.key = value.GetKey()
			doc.Type = value.GetType()
			doc.Data = value.GetData()

			if value, ok := value.GetMeta(TTL).(uint32); ok {
				expiry = value
			}

			if value, ok := value.GetMeta(CAS).(gocb.Cas); ok {
				cas = value
			}

			bulkOps[i] = &gocb.InsertOp{
				Key: doc.GetKey(),
				Value: doc,
				Expiry: expiry,
				Cas: cas,
			}
		case fmt.Stringer:
			bulkOps[i] = &gocb.InsertOp{
				Key: value.String(),
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
	for i:=0;i<length;i++{
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
	doc := newDoc()

	now := time.Now().UTC()
	doc.Meta[CREATEDON] = now
	doc.Meta[UPDATEDON] = now

	if row, ok := x.(Row); ok {

		doc.Id = row.GetId()
		doc.Type = row.GetType()
		doc.Data = row.GetData()

		doc.key = row.GetKey()

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

	for i:=0;i<length;i++ {

		doc := newDoc()
		rows[i] = doc

		switch value := xs[i].(type) {
		case string:
			bulkOps[i] = &gocb.GetOp{
				Key: value,
				Value: doc,
			}
		case Row:
			doc.key = value.GetKey()
			doc.Type = value.GetType()
			doc.Data = value.GetData()


			cas, _ := value.GetMeta(CAS).(gocb.Cas)

			bulkOps[i] = &gocb.GetOp{
				Key: doc.GetKey(),
				Cas: cas,
				Value: doc,
			}
		case fmt.Stringer:
			bulkOps[i] = &gocb.GetOp{
				Key: value.String(),
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
	for i:=0;i<length;i++{
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

	var key string
	row := newDoc()
	row.Data = out

	switch value := x.(type) {
	case string:
		key = value
	case Row:
		key = value.GetKey()
	case fmt.Stringer:
		key = value.String()
	}

	if cas, err := c.bucket.Get(key, row); err != nil {
		row.fault = err
	} else {
		row.Meta[CAS] = cas
	}

	return row
}


func (c *CouchbaseStore) Replace(xs ... interface{}) ([]Row, bool) {


	isOk := true
	length := len(xs)
	rows := make([]Row, length, length)
	bulkOps := make([]gocb.BulkOp, length, length)
	now := time.Now().UTC()


	for i:=0;i<length;i++ {
		doc := newDoc()
		rows[i] = doc

		doc.Meta[UPDATEDON] = now

		if value, ok := xs[i].(Row); ok {

			doc.key = value.GetKey()
			doc.Type = value.GetType()
			doc.Data = value.GetData()

			cas, _ := value.GetMeta(CAS).(gocb.Cas)

			expiry, _ := value.GetMeta(TTL).(uint32)

			bulkOps[i] = &gocb.ReplaceOp{
				Key: doc.GetKey(),
				Cas: cas,
				Value: doc,
				Expiry: expiry,
			}

		}else  {
			doc.fault = errors.New("Unsupported type, expecting Row.")
			isOk = false
		}
	}

	if c.bucket.Do(bulkOps) != nil {
		isOk = false
	}
	for i:=0;i<length;i++{

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

	var row *doc = newDoc()
	var expiry uint32
	var cas gocb.Cas

	now := time.Now().UTC()
	row.Meta[UPDATEDON] = now

	if value, ok := x.(Row); ok {
		if ( value.GetKey() == "" ) {
			row.Data = value.GetData()
			row.Id =  value.GetId()
		}
		if value, ok := value.GetMeta(TTL).(uint32); ok {
			expiry = value
			row.Meta[TTL] = value
		}
		if value, ok := value.GetMeta(CAS).(gocb.Cas); ok {
			cas = value
		}
	} else {
		row.Id = uuid.NewV4().String()
		row.Data = x
	}


	if cas, err := c.bucket.Replace(row.GetKey(), row, cas, expiry); err != nil {
		row.fault = err
	} else {
		row.Meta[CAS] = cas
	}

	return row
}


func (c *CouchbaseStore) Update(xs ... interface{}) ([]Row, bool) {
	var faulted bool

	var length int = len(xs)
	var rows = make([]Row, length, length)

	for i:=0;i<length;i++ {
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


func (c *CouchbaseStore) Destroy(xs...interface{}) ([]Row, bool) {
	var faulted bool

	var length int = len(xs)
	var rows = make([]Row, length, length)

	for i:=0;i<length;i++ {
		rows[i] = c.DestroyOne(xs[i])
		faulted = rows[i].IsFaulted()
	}
	return rows, faulted
}
func (c *CouchbaseStore) DestroyOne(x interface{}) Row {

	var cas gocb.Cas
	row := &doc{}

	switch value := x.(type) {
	case string:
		row.Id = value
	case Row:
		row.Id = value.GetKey()
		cas = value.GetMeta(CAS).(gocb.Cas)
	case fmt.Stringer:
		row.Id = value.String()
	}


	if cas, err := c.bucket.Remove(row.Id, cas); err != nil {
		row.fault = err
	} else {
		row.Meta[CAS] = cas
	}

	return row
}

func (c *CouchbaseStore) Exec(q Query) (QueryResult, error) {
	params := q.Params()
	if queryString, ok := q.Query().(string); !ok {
		return nil, errInvalidQueryType
	} else {

		n1qlquery := gocb.NewN1qlQuery(queryString)
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
			return newQueryResult(n1qlquery, params, results), nil
		}
	}
}
