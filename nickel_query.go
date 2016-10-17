package couchbase

import (
	"github.com/couchbase/gocb"
	domain "github.com/Tlantic/mrs-integration-domain/storage"
)

type NickelQuery struct {
	query   string
	bucket *gocb.Bucket
}

func NewNickelQuery(query string, bucket *gocb.Bucket) *NickelQuery {
	return &NickelQuery{
		query:   query,
		bucket: bucket,
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