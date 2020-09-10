package query

import "reflect"

type ModelInfoMapMaster = string

const (
	FieldName  ModelInfoMapMaster = "field_name_master"
	ColumnName ModelInfoMapMaster = "column_name_master"
)

type IModel interface {
	GetSources() ([]string, []string, []string)
}

type ModelInfo struct {
	FieldName              string
	ColumnName             string
	ReadDatabaseConverter  string
	WriteDatabaseConverter string
	ReadConverter          string
	WriteConverter         string
	Alias                  string
}

func GetModelInfo(target interface{}, master ModelInfoMapMaster) map[string]*ModelInfo {
	res := make(map[string]*ModelInfo)
	t := reflect.TypeOf(target)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		info := new(ModelInfo)
		info.FieldName = field.Name
		c := field.Tag.Get("column")
		if len(c) > 0 {
			info.ColumnName = c
		}
		c = field.Tag.Get("dbread")
		if len(c) > 0 {
			info.ReadDatabaseConverter = c
		}
		c = field.Tag.Get("dbwrite")
		if len(c) > 0 {
			info.WriteDatabaseConverter = c
		}
		c = field.Tag.Get("read")
		if len(c) > 0 {
			info.ReadConverter = c
		}
		c = field.Tag.Get("write")
		if len(c) > 0 {
			info.WriteConverter = c
		}
		c = field.Tag.Get("alias")
		if len(c) > 0 {
			info.Alias = c
		}
		switch master {
		case FieldName:
			res[info.FieldName] = info
			break
		case ColumnName:
			if len(info.Alias) > 0 {
				res[info.Alias] = info
			} else {
				res[info.ColumnName] = info
			}
			break
		default:
			panic("ModelInfoMapMaster not supported only use FieldName or ColumnName!")
		}
	}
	return res
}
