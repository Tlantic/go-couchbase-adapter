package couchbase

import (
	"os"
	"time"
	"testing"

	"github.com/twinj/uuid"
	"fmt"
)

type User struct {
	Username 	string		`json:"username"`
	Password 	string		`json:"password"`
}

func TestNewCouchbaseStore(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if ( err != nil ) {
		t.Error(err)
	}
	defer store.Close()
}

func TestCouchbaseStore_Create(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if ( err != nil ) {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username:"1",
			Password: "1",
		}

		record2 := &User{
			Username:"2",
			Password: "2",
		}

		d1 := newDoc()
		d1.SetId(uuid.NewV4().String())
		d1.SetType("test")
		d1.SetExpiry(5)
		d1.SetData(record1)


		d2 := newDoc()
		d2.SetId(uuid.NewV4().String())
		d2.SetType("test")
		d2.SetExpiry(5)
		d2.SetData(record2)

		if res, ok := store.Create(d1,d2); !ok {
			for _, v := range res {
				if v.IsFaulted() {
					t.Error(v.Fault())
				}
			}
		}

		if res, err := store.Exec(newQuery( "SELECT * FROM `m`", nil )); err != nil {
			t.Error(err)
		} else {
			usr := User{}
			if err := res.One(&struct {
				M	User
			}{
				M: usr,
			}); err != nil {
				t.Error(err)
			}
			fmt.Println(usr)
		}

	}

}

func TestCouchbaseStore_CreateOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if ( err != nil ) {
		t.Error(err)
	} else {
		defer store.Close()

		d := newDoc()
		d.SetId(uuid.NewV4().String())
		d.SetType("test")
		d.SetExpiry(5)
		d.SetData(User{
			Username:"username",
			Password: "password",
		})


		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		}

		if res := store.CreateOne(d); !res.IsFaulted() {
			t.Error("document should be faulted since it already exists one with the same key")
		}

		time.Sleep(6000 * time.Millisecond)
		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		}


		if res := store.CreateOne(d.GetData()); res.IsFaulted() {
			t.Error(res.Fault())
		}
	}

}

func TestCouchbaseStore_ReadOneWithType(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if ( err != nil ) {
		t.Error(err)
	} else {
		defer store.Close()

		record := User{
			Username:"username",
			Password: "password",
		}
		d := newDoc()
		d.SetId(uuid.NewV4().String())
		d.SetType("test")
		d.SetExpiry(5)
		d.SetData(record)


		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		} else {
			in := User{}
			if res := store.ReadOneWithType(res.GetKey(), &in); res.IsFaulted() {
				t.Error(res.Fault())
			} else {
				if ( in.Username != record.Username ) {
					t.Error("in.Username != record.Username ")
				}
				if ( in.Password != record.Password ) {
					t.Error("in.Username != record.Username ")
				}
			}
		}
	}
}

func TestCouchbaseStore_ReadOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if ( err != nil ) {
		t.Error(err)
	} else {
		defer store.Close()

		record := User{
			Username:"username",
			Password: "password",
		}
		d := newDoc()
		d.SetId(uuid.NewV4().String())
		d.SetType("test")
		d.SetExpiry(5)
		d.SetData(record)


		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		} else {
			if res := store.ReadOne(res.GetKey()); res.IsFaulted() {
				t.Error(res.Fault())
			} else {
				switch res.GetData().(type){
				case []byte:
					break
				default:
					t.Error("Expected []byte")
				}
			}
		}
	}
}

func TestCouchbaseStore_Read(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if ( err != nil ) {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username:"1",
			Password: "1",
		}

		record2 := &User{
			Username:"2",
			Password: "2",
		}

		d1 := newDoc()
		d1.SetId(uuid.NewV4().String())
		d1.SetType("test")
		d1.SetExpiry(5)
		d1.SetData(record1)


		d2 := newDoc()
		d2.SetId(uuid.NewV4().String())
		d2.SetType("test")
		d2.SetExpiry(5)
		d2.SetData(record2)

		if res := store.CreateOne(d1); res.IsFaulted() {
			t.Error(res.Fault())
		}
		if res := store.CreateOne(d2); res.IsFaulted() {
			t.Error(res.Fault())
		}

		if res, ok := store.Read(d1, d2); !ok {
			for _,v := range res {
				if ( v.IsFaulted() )  {
					t.Error(v.Fault())
				}
			}
		}
	}
}

func TestCouchbaseStore_Exec(t *testing.T) {

}