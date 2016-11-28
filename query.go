package couchbase

import (
	"sync"
	"bytes"
	"encoding/json"
	"github.com/couchbase/gocb"
	"github.com/Tlantic/mrs-integration-domain/storage"
)

// Assert interface implementation
var _ storage.QueryResult = (*query)(nil)


type query struct {
	locker 		sync.Mutex
	query		interface{}
	params		interface{}
	meta		map[string]interface{}
	index		uint64
	data 		[][]byte
}
func newQuery (q interface{}, p interface{}) *query {
	return &query{
		query: q,
		params: p,
		meta: make(map[string]interface{}),
	}
}
func newQueryResult( q interface{}, p interface{}, r gocb.QueryResults) *query {

	data := make([][]byte, 0)
	for b := r.NextBytes(); b != nil; b = r.NextBytes() {
		data = append(data, b)
	}

	return &query{
		query: q,
		params: p,
		data: data,
		meta: make(map[string]interface{}),
	}
}

func (m *query) unshift() []byte {
	m.locker.Lock()
	defer m.locker.Unlock()

	if ( len(m.data) > 0 ) {
		var elem []byte
		elem, m.data = m.data[0], m.data[1:]
		return elem
	}
	return nil
}
func (m *query) unshiftN( n int) [][]byte {
	m.locker.Lock()
	defer m.locker.Unlock()

	length := len(m.data)
	if ( length > 0 ) {

		var elem [][]byte

		if length < n {
			n = length
		}

		elem, m.data = m.data[0:n], m.data[n:]
		return elem
	}

	return nil
}
//noinspection GoReservedWordUsedAsName
func (m *query) copy() [][]byte {
	m.locker.Lock()
	defer m.locker.Unlock()
	return append([][]byte(nil), m.data...)
}


func (m *query) One( ref interface{} ) error{
	if elem := m.unshift(); elem != nil {
		if err := json.NewDecoder(bytes.NewReader(elem)).Decode(ref); err != nil {
			return err
		}
	}
	return nil
}

func (m *query) OneBytes() []byte {
	if elem := m.unshift(); elem != nil {
		return elem
	}
	return nil
}

func (m *query) Take( n int ) storage.QueryResult {
	return &query{
		data: m.unshiftN( n ),
	}
}

func (m *query) Skip( n int ) storage.QueryResult {
	m.unshiftN( n )
	return m
}

func (m *query) ForEach( eachFunc func(int, []byte) ) {
	data := m.copy()
	length := len(data)
	if  length > 0  {
		for i:=0;i<length;i++{
			eachFunc(i, data[i])
		}
	}
}

func (m *query) Map( mapFunc func(int, []byte) interface{} ) []interface{} {

	tmp := m.copy()
	length := len(tmp)
	data := make([]interface{}, length, length)
	if  length > 0  {
		for i:=0;i<length;i++{
			data[i] = mapFunc(i, tmp[i])
		}
	}
	return data
}

func (m *query) Range() <- chan []byte {
	data := m.copy()
	length := len(data)
	c := make(chan []byte, length)
	go func(data [][]byte, length int) {
		for i:=0;i<length;i++{
			c <- data[i]
		}
	}(data, length)
	return c
}



func (m *query) Query() interface {} {
	return m.query
}

func (m *query) Params() interface{} {
	return m.params
}

func (m *query) SetMeta(key string, value interface{}) {
	m.meta[key] = value
}

func (m *query) GetMeta(key string) interface{} {
	return m.meta[key]
}

func (m *query) Close() error {
	m.data = nil
	return nil
}