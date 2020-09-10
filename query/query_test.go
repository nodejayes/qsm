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
	ID       int       `column:"tt.id"`
	Age      int       `column:"tt.age->addOneConverter"`
	Height   float64   `column:"tt.height"`
	Name     string    `column:"tt.name"`
	Birthday time.Time `column:"tt.birthday"`
	Dyn      DynStruct `column:"tt.dyn"`
	Dynb     DynStruct `column:"tt.dynb"`
	Active   bool      `column:"tt.active" alias:"ac"`
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
	q := New(connection.New(cfg.TestConnection, "postgres"))
	q.RegisterColumnConvert("addOneConverter", "$column + 1")
	var res []TestTypes
	tmp, err := q.Select(TestTypes{}, "", -1, -1)
	err = mapstructure.Decode(tmp, &res)
	if err != nil {
		t.Errorf("expect err to be nil but was: %v", err.Error())
		return
	}
}

func TestSelectVersion(t *testing.T) {
	q := New(connection.New(cfg.TestConnection, "postgres"))
	q2 := New(connection.New(cfg.TestConnection, "postgres"))
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

func TestParameter(t *testing.T) {
	q := New(connection.New(cfg.TestConnection, "postgres"))
	_, _ = q.Select(Db{}, "where test = :test and item = :item and arrtime = :arrtime and time = :time and float = :float and arrstr = :arrstring and arrint = :arrint and arrfloat = :arrfloat and injection = :injection", -1, -1, map[string]interface{}{
		"test":      5,
		"float":     2.5,
		"item":      "xxxxxx",
		"time":      time.Date(2020, 1, 1, 20, 15, 36, 0, time.UTC),
		"arrint":    []int{1, 2, 3},
		"arrfloat":  []float64{1.5, 2.5, 3.5},
		"arrstring": []string{"1", "a", "b"},
		"arrtime":   []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
		"injection": "' and 1 = 1",
	})
}
