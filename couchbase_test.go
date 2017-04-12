package couchbase

import (
	"encoding/json"
	"os"
	"testing"
	"time"
	"bytes"
	"github.com/Tlantic/go-nosql/database"
	"github.com/twinj/uuid"
	"strconv"
)

func _firstFault(rows []database.Row) error {
	for _, v := range rows {
		if v.IsFaulted() {
			return v.Fault()
		}
	}

	return nil
}

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func TestNewCouchbaseStore(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		store.Close()
	}
}

func TestCouchbaseStore_Create(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		record2 := &User{
			Username: "2",
			Password: "2",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetType("test")
		d1.SetExpiry(1)
		d1.SetData(record1)

		d2 := newDoc(uuid.NewV4().String())
		d2.SetType("test")
		d2.SetMeta(database.TTL, 1)
		d2.SetData(record2)

		if res, ok := store.Create(d1, d2); !ok {
			t.Error(_firstFault(res))
		}
	}

}

func TestCouchbaseStore_DestroyOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetType("test")
		d1.SetExpiry(1)
		d1.SetData(record1)

		if row := store.CreateOne(d1); row.IsFaulted() {
			t.Error(row.Fault())
		} else if row = store.DestroyOne(row); row.IsFaulted() {
			t.Fatal(row.Fault())
		}
	}
}

func TestCouchbaseStore_Destroy(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetExpiry(1)
		d1.SetData(record1)

		d2 := newDoc(uuid.NewV4().String())
		d2.SetExpiry(1)
		d2.SetData(record1)

		d3 := newDoc(uuid.NewV4().String())
		d3.SetExpiry(1)
		d3.SetData(record1)

		if rows, ok := store.Create(d1, d2, d3); !ok {
			t.Error("Error creating records...", _firstFault(rows))
		} else if rs, ok := store.Destroy(d1, d2, d3); !ok {
			t.Error(_firstFault(rs))
		}
	}
}

func TestCouchbaseStore_CreateOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		d := newDoc(uuid.NewV4().String())
		d.SetExpiry(1)
		d.SetData(&User{
			Username: "username",
			Password: "password",
		})

		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		}

		if res := store.CreateOne(d); !res.IsFaulted() {
			t.Error("document should be faulted since it already exists one with the same key")
		}

		time.Sleep(2 * time.Second)
		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		}

		if res := store.CreateOne(d.GetData()); res.IsFaulted() {
			t.Error(res.Fault())
		} else if res = store.DestroyOne(res); res.IsFaulted() {
			t.Fatal(res.Fault())
		}

	}

}

func TestCouchbaseStore_ReadOneWithType(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record := &User{
			Username: "username",
			Password: "password",
		}
		d := newDoc(uuid.NewV4().String())
		d.SetExpiry(1)
		d.SetData(record)

		if res := store.CreateOne(d); res.IsFaulted() {
			t.Error(res.Fault())
		} else {
			in := User{}
			if res := store.ReadOneWithType(res.GetKey(), &in); res.IsFaulted() {
				t.Error(res.Fault())
			} else {
				if in.Username != record.Username {
					t.Error("in.Username != record.Username ")
				}
				if in.Password != record.Password {
					t.Error("in.Username != record.Username ")
				}
			}
		}
	}
}

func TestCouchbaseStore_ReadOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record := &User{
			Username: "username",
			Password: "password",
		}
		d := newDoc(uuid.NewV4().String())
		d.SetExpiry(1)
		d.SetData(record)

		 res := store.CreateOne(d);
		if res.IsFaulted()  {
			t.Error(res.Fault())
			return
		}

		if res := store.ReadOne(res.GetKey()); res.IsFaulted() {
				t.Error(res.Fault())
		} else {
			switch v := res.GetData().(type) {
			case []byte:
				u := &User{}
				if err := json.NewDecoder(bytes.NewBuffer(v)).Decode(u); err != nil {
					t.Error(err.Error())
				}

				if u.Username != "username" {
					t.Errorf("Expected username to be username. Got %s.\n", u.Username)
				}
				if u.Password != "password" {
					t.Errorf("Expected username to be password. Got %s.\n", u.Password)
				}
			default:
				t.Error("Expected []byte")
			}
		}

		res.SetMeta(database.LOCK, 5)
		res.SetData(&User{})
		if res := store.ReadOne(res); res.IsFaulted() {
			t.Error(res.Fault())
		} else {
			switch  res.GetData().(type) {
			case *User:
				res.SetMeta(database.CAS, nil)
				res.SetMeta(database.TTL, 1)
				if res2 := store.TouchOne(res); !res2.IsFaulted() {
					t.Fatal("Expected error")
				}
				break
			default:
				t.Error("Expected *User")
			}
		}
	}
}

func TestCouchbaseStore_Read(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		record2 := &User{
			Username: "2",
			Password: "2",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetExpiry(1)
		d1.SetData(record1)

		d2 := newDoc(uuid.NewV4().String())
		d2.SetExpiry(1)
		d2.SetData(record2)

		if res := store.CreateOne(d1); res.IsFaulted() {
			t.Error(res.Fault())
		}
		if res := store.CreateOne(d2); res.IsFaulted() {
			t.Error(res.Fault())
		}

		if res, ok := store.Read(d1, d2); !ok {
			for _, v := range res {
				if v.IsFaulted() {
					t.Error(v.Fault())
				}
			}
		}

		d1.SetMeta(database.LOCK, 15)

		if res := store.ReadOne(d1); res.IsFaulted() {
			t.Error(res.Fault())
		}
		d2.SetMeta(database.LOCK, 15)
		if res := store.ReadOne(d2); res.IsFaulted() {
			t.Error(res.Fault())
		}

		d1.SetMeta(database.CAS, nil)
		d2.SetMeta(database.CAS, nil)
		if rows, ok := store.Replace(d1, d2); ok {
			t.Error("expected documents to be locked.", _firstFault(rows))
		}
	}
}

func TestCouchbaseStore_ReplaceOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetType("test")
		d1.SetExpiry(1)
		d1.SetData(record1)

		if res := store.CreateOne(d1); res.IsFaulted() {
			t.Error(res.Fault())
		}

		record1.Password = "pass"

		if res := store.ReplaceOne(d1); res.IsFaulted() {
			t.Fatal(res.Fault())
		} else if res = store.ReadOne(res); res.IsFaulted() {
			t.Fatal(res.Fault())
		} else if res.GetData().(*User).Password != "pass" {
			t.Error("Failed to replace password")
		}
		time.Sleep(1 * time.Second)
	}
}

func TestCouchbaseStore_Replace(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		record2 := &User{
			Username: "2",
			Password: "2",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetType("test")
		d1.SetExpiry(1)
		d1.SetData(record1)

		d2 := newDoc(uuid.NewV4().String())
		d2.SetType("test")
		d2.SetMeta(database.TTL, 1)
		d2.SetData(record2)

		if res, ok := store.Create(d1, d2); !ok {
			t.Error(_firstFault(res))
		}

		record1.Password = "0"
		record2.Password = "1"

		if res, ok := store.Replace(d1, d2); !ok {
			t.Error(_firstFault(res))
		} else {
			for i, r := range res {
				if r.GetData().(*User).Password != strconv.FormatInt(int64(i), 10) {
					t.Error("Failed to replace passwords")
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func TestCouchbaseStore_TouchOne(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record := &User{
			Username: "username",
			Password: "password",
		}
		d := newDoc(uuid.NewV4().String())
		d.SetExpiry(1)
		d.SetData(record)
		d.SetMeta("Func", "TestCouchbaseStore_TouchOne")

		res := store.CreateOne(d);
		if res.IsFaulted()  {
			t.Error(res.Fault())
			return
		}

		res.SetMeta(database.TTL, 1)
		if res2 := store.TouchOne(res); res2.IsFaulted() {
			t.Error(res2.Fault())
		}

		res.SetMeta(database.LOCK, 5)
		res.SetData(&User{})
		if res := store.ReadOne(res); res.IsFaulted() {
			t.Error(res.Fault())
		} else {
			switch  res.GetData().(type) {
			case *User:
				res.SetMeta(database.CAS, nil)
				res.SetMeta(database.TTL, 1)
				if res = store.TouchOne(res); !res.IsFaulted() {
					t.Fatal("Expected an error. doc is locked")
				}
				break
			default:
				t.Error("Expected *User")
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func TestCouchbaseStore_Touch(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {
		defer store.Close()

		record1 := &User{
			Username: "1",
			Password: "1",
		}

		record2 := &User{
			Username: "2",
			Password: "2",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetExpiry(1)
		d1.SetData(record1)
		d1.SetMeta("Func", "TestCouchbaseStore_Touch")
		d2 := newDoc(uuid.NewV4().String())
		d2.SetExpiry(1)
		d2.SetData(record2)
		d2.SetMeta("Func", "TestCouchbaseStore_Touch")

		if res := store.CreateOne(d1); res.IsFaulted() {
			t.Error(res.Fault())
		}
		if res := store.CreateOne(d2); res.IsFaulted() {
			t.Error(res.Fault())
		}

		d1.SetMeta(database.TTL, 1)
		d2.SetMeta(database.TTL, 1)
		if res, ok := store.Touch(d1, d2); ok {
			if res[0].GetMeta(database.CAS) == d1.GetMeta(database.CAS) {
				t.Error("expected cas to be different")
			}
			if res[1].GetMeta(database.CAS) == d2.GetMeta(database.CAS) {
				t.Error("expected cas to be different")
			}
		} else {
			t.Error(_firstFault(res))
		}

		d1.SetMeta(database.LOCK, 15)

		if res := store.ReadOne(d1); res.IsFaulted() {
			t.Error(res.Fault())
		}
		d2.SetMeta(database.LOCK, 15)
		if res := store.ReadOne(d2); res.IsFaulted() {
			t.Error(res.Fault())
		}

		d1.SetMeta(database.CAS, nil)
		d2.SetMeta(database.CAS, nil)
		d1.SetMeta(database.TTL, 1)
		d2.SetMeta(database.TTL, 1)
		if rows, ok := store.Touch(d1, d2); ok {
			t.Error("expected documents to be locked.", _firstFault(rows))
		}
	}
}

func TestCouchbaseStore_Exec(t *testing.T) {
	store, err := NewCouchbaseStore(os.Getenv("COUCHBASE_HOST"), os.Getenv("COUCHBASE_BUCKET"), os.Getenv("COUCHBASE_PASSWORD"))
	if err != nil {
		t.Error(err)
	} else {

		defer store.Close()
		time.Sleep(5 * time.Second)
		record1 := &User{
			Username: "1",
			Password: "1",
		}

		record2 := &User{
			Username: "2",
			Password: "2",
		}

		d1 := newDoc(uuid.NewV4().String())
		d1.SetExpiry(1)
		d1.SetData(record1)
		d1.SetMeta("Func", "TestCouchbaseStore_Exec")

		d2 := newDoc(uuid.NewV4().String())
		d2.SetExpiry(1)
		d2.SetData(record2)
		d2.SetMeta("Func", "TestCouchbaseStore_Exec")

		if res, ok := store.Create(d1, d2); !ok {
			for _, v := range res {
				if v.IsFaulted() {
					t.Error(v.Fault())
				}
			}
		}

		q := store.NewQuery("SELECT * FROM `test` WHERE _type=\"user\"")
		q.SetMeta(database.CONSISTENCY, 2)

		if res, err := store.Exec(q); err != nil {
			t.Error(err)
		} else {

			users := []User{}

			res.ForEach(func(idx int, data []byte) {
				var result struct {
					Row struct {
						Data User `json:"data"`
					} `json:"test"`
				}
				json.Unmarshal(data, &result)
				users = append(users, result.Row.Data)
			})

			if len(users) != 2 {
				t.Errorf("Expecting query to return 2 rows. Returned %d.\n", len(users))
			}
		}

	}
}
