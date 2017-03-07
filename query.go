package couchbase

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/Tlantic/go-nosql/database"
	"github.com/couchbase/gocb"
)

// Assert interface implementation
var (
	_ database.QueryResult = (*queryResult)(nil)
	_ database.Query       = (*query)(nil)
)

type query struct {
	statement string
	params    interface{}
	meta      map[string]interface{}
}

func newQuery(statement string) *query {
	return &query{
		statement: statement,
		meta:      make(map[string]interface{}),
	}
}

func (q *query) GetStatement() string {
	return q.statement
}
func (q *query) SetStatement(query string) {
	q.statement = query
}

func (q *query) GetParams() interface{} {
	return q.params
}
func (q *query) SetParams(params interface{}) {
	q.params = params
}

func (q *query) SetMeta(key string, value interface{}) {
	q.meta[key] = value
}
func (q *query) GetMeta(key string) interface{} {
	return q.meta[key]
}

type queryResult struct {
	database.Query
	locker sync.Mutex
	index  uint64
	data   [][]byte
}

func newQueryResult(q database.Query, r gocb.QueryResults) *queryResult {
	data := make([][]byte, 0)
	for b := r.NextBytes(); b != nil; b = r.NextBytes() {
		data = append(data, b)
	}
	return &queryResult{
		Query: q,
		data:  data,
	}
}

//noinspection GoReservedWordUsedAsName
func (q *queryResult) copy() [][]byte {
	q.locker.Lock()
	defer q.locker.Unlock()
	return append([][]byte(nil), q.data...)
}
func (q *queryResult) unshift() []byte {
	q.locker.Lock()
	defer q.locker.Unlock()

	if ( len(q.data) > 0 ) {
		var elem []byte
		elem, q.data = q.data[0], q.data[1:]
		return elem
	}
	return nil
}
func (q *queryResult) unshiftN(n int) [][]byte {
	q.locker.Lock()
	defer q.locker.Unlock()

	length := len(q.data)
	if ( length > 0 ) {

		var elem [][]byte

		if length < n {
			n = length
		}

		elem, q.data = q.data[0:n], q.data[n:]
		return elem
	}

	return nil
}

func (q *queryResult) One(out interface{}) error {
	if elem := q.unshift(); elem != nil {
		if err := json.NewDecoder(bytes.NewReader(elem)).Decode(out); err != nil {
			return err
		}
	}
	return nil
}

func (q *queryResult) OneBytes() []byte {
	if elem := q.unshift(); elem != nil {
		return elem
	}
	return nil
}

func (q *queryResult) Take(n int) database.QueryResult {
	return &queryResult{
		Query: q.Query,
		data:  q.unshiftN(n),
	}
}

func (q *queryResult) Skip(n int) database.QueryResult {
	q.unshiftN(n)
	return q
}

func (q *queryResult) ForEach(eachFunc func(int, []byte)) {
	data := q.copy()
	length := len(data)
	for i := 0; i < length; i++ {
		eachFunc(i, data[i])
	}
}

func (q *queryResult) Map(mapFunc func(int, []byte) interface{}) []interface{} {

	tmp := q.copy()
	length := len(tmp)
	data := make([]interface{}, length, length)
	if length > 0 {
		for i := 0; i < length; i++ {
			data[i] = mapFunc(i, tmp[i])
		}
	}
	return data
}

func (q *queryResult) Range() <-chan []byte {
	data := q.copy()
	length := len(data)
	c := make(chan []byte, length)
	go func(data [][]byte, length int) {
		for i := 0; i < length; i++ {
			c <- data[i]
		}
	}(data, length)
	return c
}

func (q *queryResult) Close() error {
	q.data = nil
	return nil
}
