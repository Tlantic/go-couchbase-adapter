package couchbase

import (
	"testing"
	"errors"
	"github.com/Tlantic/mrs-integration-domain/storage"
	"time"
)


func TestDoc_SetKey(t *testing.T) {
	key := "1337"
	doc := newDoc(key)

	doc.SetKey(key)
	if doc.key != key {
		t.Fail()
	}
}

func TestDoc_GetKey(t *testing.T) {
	key := "1337"
	doc := newDoc(key)

	doc.SetKey(key)
	if doc.GetKey() != key {
		t.Fail()
	}
}

func TestDoc_SetData(t *testing.T) {
	data := &map[string]string{
		"propName": "propValue",
	}
	doc := newDoc("")

	doc.SetData(data)
	if doc.Data != data {
		t.Fail()
	}
}

func TestDoc_GetData(t *testing.T) {
	data := &map[string]string{
		"propName": "propValue",
	}
	doc := newDoc("")

	doc.SetData(data)
	if doc.GetData() != data {
		t.Fail()
	}
}

func TestDoc_SetMeta(t *testing.T) {
	doc := newDoc("")
	doc.SetMeta("propName", "propValue")
	if doc.Meta["propName"].(string) != "propValue" {
		t.Fail()
	}
}

func TestDoc_GetMeta(t *testing.T) {
	doc := newDoc("")
	doc.SetMeta("propName", "propValue")
	if doc.GetMeta("propName").(string) != "propValue" {
		t.Fail()
	}
}

func TestDoc_CreatedOn(t *testing.T) {
	var ts *time.Time

	now := time.Now()
	doc := newDoc("")

	ts = doc.CreatedOn()
	if (ts != nil) {
		t.Fail()
	}

	doc.SetMeta(storage.CREATEDON, "string")
	ts = doc.CreatedOn()
	if (ts != nil) {
		t.Fail()
	}


	doc.SetMeta(storage.CREATEDON, now.UnixNano())
	ts = doc.CreatedOn()
	if (*ts != now) {
		t.Fail()
	}

	doc.SetMeta(storage.CREATEDON, now)
	ts = doc.CreatedOn()
	if (*ts != now) {
		t.Fail()
	}
}

func TestDoc_IsFaulted(t *testing.T) {
	doc := newDoc("")

	if doc.IsFaulted() == true {
		t.FailNow()
	} else {
		doc.fault = errors.New("errMessage")
	}

	if ( doc.IsFaulted() == false ) {
		t.FailNow()
	}
}

func TestDoc_Fault(t *testing.T) {
	doc := newDoc("")
	fault := errors.New("errMessage")
	doc.fault = fault


	if ( doc.Fault() != fault ) {
		t.FailNow()
	}
}

func TestDoc_SetExpiry(t *testing.T) {
	doc := newDoc("")

	doc.SetExpiry(1)
	if doc.GetMeta(storage.TTL).(uint32) != 1 {
		t.FailNow()
	}

}