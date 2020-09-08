package converter

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

func ReadBool(dbValue interface{}, typ *sql.ColumnType, field reflect.StructField, columnName string, result *map[string]interface{}) error {
	if v, ok := dbValue.(bool); ok {
		(*result)[field.Name] = v
		return nil
	}
	SetDefaultValue(result, field)
	return nil
}

func WriteBool(fieldValue interface{}, typ *sql.ColumnType, field reflect.StructField, columnName string, result *map[string]interface{}) error {
	if field.Type.Kind() == reflect.Bool {
		v, ok := fieldValue.(bool)
		if !ok {
			return errors.New(fmt.Sprintf("can't convert %v: %v to bool", field.Name, fieldValue))
		}
		(*result)[columnName] = v
		return nil
	}
	return nil
}
