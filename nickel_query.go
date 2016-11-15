package couchbase

import (
	domain "github.com/Tlantic/mrs-integration-domain/storage"
	"github.com/couchbase/gocb"
)

type NickelQuery struct {
	query  string
	params interface{}
	bucket *gocb.Bucket
}

func NewNickelQuery(query string, bucket *gocb.Bucket) *NickelQuery {
	return &NickelQuery{
		query:  query,
		bucket: bucket,
	}
}

// NewNickelQueryWithParams ...
func NewNickelQueryWithParams(query string, bucket *gocb.Bucket, params interface{}) *NickelQuery {
	return &NickelQuery{
		query:  query,
		bucket: bucket,
		params: params,
	}
}

func (n *NickelQuery) Execute() (error, []*domain.DbObject) {
	query := gocb.NewN1qlQuery(n.query)
	rows, err := n.bucket.ExecuteN1qlQuery(query, nil)
	if err != nil {
		return err, nil
	}

	var document interface{}
	var documents []*domain.DbObject
	for rows.Next(&document) {
		doc := &domain.DbObject{
			Data: document,
		}
		documents = append(documents, doc)
	}

	err = rows.Close()
	if err != nil {
		return err, nil
	}

	return nil, documents
}

// ExecuteWithParams ...
func (n *NickelQuery) ExecuteWithParams() ([]*domain.DbObject, error) {
	query := gocb.NewN1qlQuery(n.query)
	rows, err := n.bucket.ExecuteN1qlQuery(query, n.params)
	if err != nil {
		return nil, err
	}

	var document interface{}
	var documents []*domain.DbObject
	for rows.Next(&document) {
		doc := &domain.DbObject{
			Data: document,
		}
		documents = append(documents, doc)
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return documents, nil
}
