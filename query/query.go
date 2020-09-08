package query

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/nodejayes/qsm/connection"
	"github.com/nodejayes/qsm/converter"
	"reflect"
	"strconv"
	"strings"
)

func New(connection *connection.Connection) *Api {
	me := &Api{
		connection: connection,
		converters: make(map[string]ConverterFunction),
	}
	me.RegisterConverter("ReadBool", converter.ReadBool)
	me.RegisterConverter("WriteBool", converter.WriteBool)
	return me
}

type ConverterFunction = func(dbValue interface{}, typ *sql.ColumnType, field reflect.StructField, columnName string, result *map[string]interface{}) error

type Api struct {
	connection *connection.Connection
	converters map[string]ConverterFunction
}

func (ctx *Api) RegisterConverter(name string, converter ConverterFunction) {
	ctx.converters[name] = converter
}

func (ctx *Api) UnregisterConverter(name string) {
	delete(ctx.converters, name)
}

func (ctx *Api) Select(target IModel, where string, limit, offset int, args ...interface{}) ([]map[string]interface{}, error) {
	var res []map[string]interface{}
	query := ctx.generateSelect(target, where, limit, offset)

	if !ctx.connection.IsConnected() {
		ctx.connection.Connect(50)
	}
	rows, err := ctx.connection.GetInstance().Query(query, args...)
	if rows == nil {
		return nil, errors.New("missing database rows instance")
	}
	defer func() {
		_ = rows.Close()
	}()

	if err != nil {
		return res, err
	}

	return ctx.fillResultRows(target, rows)
}

func (ctx *Api) generateSelect(target IModel, where string, limit, offset int) string {
	info := GetModelInfo(target, FieldName)
	types, sources, aliases := target.GetSources()
	buf := bytes.NewBuffer([]byte{})
	buf.WriteString("select ")
	counter := 0
	for _, infos := range info {
		columnName := infos.ColumnName
		if len(infos.Src) > 0 {
			columnName = "\"" + infos.Src + "\".\"" + columnName + "\""
		}
		if counter > 0 {
			buf.WriteString(", ")
		}
		if len(infos.ReadDatabaseConverter) > 0 {
			buf.WriteString(strings.ReplaceAll(infos.ReadDatabaseConverter, "$column", columnName))
			buf.WriteString(" as \"")
			buf.WriteString(columnName)
			buf.WriteString("\"")
		} else {
			buf.WriteString(columnName)
		}
		counter++
	}

	for idx := range sources {
		buf.WriteString(" ")
		buf.WriteString(types[idx])
		buf.WriteString(" ")
		buf.WriteString("\"" + strings.ReplaceAll(sources[idx], ".", "\".\"") + "\"")
		if len(aliases[idx]) > 0 {
			buf.WriteString(" ")
			buf.WriteString(aliases[idx])
		}
	}

	buf.WriteString(" ")
	buf.WriteString(where)
	if limit > -1 {
		buf.WriteString(" limit ")
		buf.WriteString(strconv.FormatInt(int64(limit), 10))
	}
	if offset > 0 {
		buf.WriteString(" offset ")
		buf.WriteString(strconv.FormatInt(int64(offset), 10))
	}
	return buf.String()
}

func (ctx *Api) fillResultRows(target IModel, rows *sql.Rows) ([]map[string]interface{}, error) {
	var res []map[string]interface{}
	infos := GetModelInfo(target, ColumnName)
	s := reflect.TypeOf(target)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}

	for rows.Next() {
		elem := make(map[string]interface{})

		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		types, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		scanResult, scanErr := ctx.scanDbValues(rows, columns)
		if scanErr != nil {
			return nil, err
		}

		for idx := range columns {
			info := infos[columns[idx]]

			f, ok := s.FieldByName(info.FieldName)
			if !ok {
				return nil, errors.New(fmt.Sprintf("can't get field info for field %v in struct %v", info.FieldName, s.Name()))
			}

			conv := ctx.converters[info.ReadConverter]
			if conv == nil {
				elem[info.FieldName] = scanResult[idx]
				continue
			}
			err := conv(scanResult[idx], types[idx], f, columns[idx], &elem)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("error in converter %v: %v", info.ReadConverter, err.Error()))
			}
		}
		res = append(res, elem)
	}
	return res, nil
}

func (ctx *Api) scanDbValues(rows *sql.Rows, columnNames []string) ([]interface{}, error) {
	values := make([]interface{}, len(columnNames))
	valuesPtr := make([]interface{}, len(columnNames))
	for idx := range columnNames {
		valuesPtr[idx] = &values[idx]
	}
	scanErr := rows.Scan(valuesPtr...)
	return values, scanErr
}
