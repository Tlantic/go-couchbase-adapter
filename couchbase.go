package couchbase

import (
	"github.com/couchbase/gocb"
	"github.com/twinj/uuid"
	domain "github.com/Tlantic/mrs-integration-domain/storage"
	"reflect"
)

type CouchbaseStore struct {
	name           string
	host           string
	bucketName     string
	bucketUser     string
	bucketPassword string
	bucket         *gocb.Bucket
	cluster        *gocb.Cluster
}

//noinspection GoUnusedExportedFunction
func NewCouchbaseStore(host, bucketName, bucketUser, bucketPassword string) *CouchbaseStore {
	s := &CouchbaseStore{
		name:           "couchbase",
		host:           host,
		bucketName:     bucketName,
		bucketUser:     bucketUser,
		bucketPassword: bucketPassword,
	}

	if err := s.ConnectBucket(); err != nil {
		panic(err)
	}

	return s
}

func (c *CouchbaseStore) ConnectBucket() error {
	cluster, err := gocb.Connect(c.host)
	if err != nil {
		return err
	}


	b, err := cluster.OpenBucket(c.bucketName, c.bucketPassword)
	if err != nil {
		return err
	}

	c.bucket = b
	c.cluster = cluster
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
	data =  reflect.New(reflect.TypeOf(data).Elem()).Interface()
	_, err := c.bucket.Get(key, data)
	if err != nil {
		return err, nil
	}

	obj := &domain.DbObject{
		Key:      key,
		Data:     data,
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
		Key:      key,
		Data:     data,
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