package query

import (
	_ "github.com/lib/pq"
	"github.com/mitchellh/mapstructure"
	"github.com/nodejayes/qsm/cfg"
	"github.com/nodejayes/qsm/connection"
	"testing"
	"time"
)

type Db struct {
	Version string `src:"v" column:"version"`
}

func (ctx Db) GetSources() ([]string, []string, []string) {
	return []string{
			"from",
		}, []string{
			"(select version())",
		}, []string{
			"v",
		}
}

type DynStruct struct {
	Hello string `json:"hello" bson:"hello"`
}

type TestTypes struct {
	ID       int       `src:"tt" column:"id"`
	Age      int       `src:"tt" column:"age" dbread:"($column) + 1"`
	Height   float64   `src:"tt" column:"height"`
	Name     string    `src:"tt" column:"name"`
	Birthday time.Time `src:"tt" column:"birthday"`
	Dyn      DynStruct `src:"tt" column:"dyn"`
	Dynb     DynStruct `src:"tt" column:"dynb"`
	Active   bool      `src:"tt" column:"active"`
}

func (ctx TestTypes) GetSources() ([]string, []string, []string) {
	return []string{
			"from",
		}, []string{
			"public.test_types",
		}, []string{
			"tt",
		}
}

func TestApi_Select(t *testing.T) {
	q := New(connection.New(cfg.TestConnectionString, "postgres"))
	var res []TestTypes
	tmp, err := q.Select(TestTypes{}, "", -1, -1)
	err = mapstructure.Decode(tmp, &res)
	if err != nil {
		t.Errorf("expect err to be nil but was: %v", err.Error())
		return
	}
}

func TestSelectVersion(t *testing.T) {
	q := New(connection.New(cfg.TestConnectionString, "postgres"))
	q2 := New(connection.New(cfg.TestConnectionString, "postgres"))
	var res []Db
	tmp, err := q.Select(Db{}, "", -1, -1)
	tmp, err = q2.Select(Db{}, "", -1, -1)
	err = mapstructure.Decode(tmp, &res)
	if err != nil {
		t.Errorf("expect err to be nil but was: %v", err.Error())
		return
	}
	if len(res) != 1 {
		t.Errorf("expect one row in result slice")
		return
	}
	if len(res[0].Version) < 1 {
		t.Errorf("Version has no value but expect one")
		return
	}
}
