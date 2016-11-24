package couchbase

import (
	"reflect"

	domain "github.com/Tlantic/mrs-integration-domain/storage"
	"github.com/couchbase/gocb"
	"github.com/twinj/uuid"
	"sync"
)

var mu = sync.Mutex{}
var clusters = map[string]*gocb.Cluster{}

type CouchbaseStore struct {
	name     string
	bucket   *gocb.Bucket
}

//noinspection GoUnusedExportedFunction,GoUnusedParameter
func NewCouchbaseStore(host, bucketName, bucketUser, bucketPassword string) (*CouchbaseStore, error) {
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
		return &CouchbaseStore{
			name:   "couchbase",
			bucket: b,
		}, nil
	}
	return nil, err
}

func (c *CouchbaseStore) ConnectBucket() error {
	return nil
}

func (c *CouchbaseStore) ShutdownBucket() {
	c.bucket.Close()
}

func (c *CouchbaseStore) GetName() string {
	return c.name
}

func (c *CouchbaseStore) SetName(name string) error {
	c.name = name
	return nil
}

func (c *CouchbaseStore) Create(obj domain.DbObject) error {

	if obj.Key == "" {
		obj.Key = uuid.NewV4().String()
	}
	_, err := c.bucket.Insert(obj.Key, obj.Data, obj.Expiry)
	if err != nil {
		return err
	}

	return nil
}

func (c *CouchbaseStore) ReadOneWithType(key string, data interface{}) (error, *domain.DbObject) {
	data = reflect.New(reflect.TypeOf(data).Elem()).Interface()
	_, err := c.bucket.Get(key, data)
	if err != nil {
		return err, nil
	}

	obj := &domain.DbObject{
		Key:  key,
		Data: data,
	}

	return nil, obj
}

func (c *CouchbaseStore) ReadOne(key string) (error, *domain.DbObject) {
	var data interface{}
	_, err := c.bucket.Get(key, &data)
	if err != nil {
		return err, nil
	}

	obj := &domain.DbObject{
		Key:  key,
		Data: data,
	}

	return nil, obj
}

func (c *CouchbaseStore) UpdateOne(obj *domain.DbObject) error {
	_, err := c.bucket.Replace(obj.Key, obj.Data, 0, obj.Expiry)
	if err != nil {
		return err
	}

	return nil
}

func (c *CouchbaseStore) Update(obj *domain.DbObject) error {
	_, err := c.bucket.Replace(obj.Key, obj.Data, 0, obj.Expiry)
	if err != nil {
		return err
	}

	return nil
}

func (c *CouchbaseStore) DestroyOne(key string) error {
	// We do not need to keep the ID that this returns.
	_, err := c.bucket.Remove(key, 0)
	if err != nil {
		return err
	}

	return nil
}

func (c *CouchbaseStore) Destroy(data *domain.DbObject) error {
	return nil
}

func (c *CouchbaseStore) Read(query string) (error, []*domain.DbObject) {
	qyr := NewNickelQuery(query, c.bucket)
	return qyr.Execute()
}

func (c *CouchbaseStore) Exec(query string, params interface{}) ([]*domain.DbObject, error) {
	qyr := NewNickelQueryWithParams(query, c.bucket, params)
	return qyr.ExecuteWithParams()
}
