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
	"time"
)

func New(connection *connection.Connection) *Api {
	me := &Api{
		connection:      connection,
		converters:      make(map[string]ConverterFunction),
		columnConverter: make(map[string]string),
	}
	me.RegisterConverter("ReadBool", converter.ReadBool)
	me.RegisterConverter("WriteBool", converter.WriteBool)
	return me
}

type ConverterFunction = func(dbValue interface{}, typ *sql.ColumnType, field reflect.StructField, columnName string, result *map[string]interface{}) error

type Api struct {
	connection      *connection.Connection
	converters      map[string]ConverterFunction
	columnConverter map[string]string
}

func (ctx *Api) RegisterConverter(name string, converter ConverterFunction) {
	ctx.converters[name] = converter
}

func (ctx *Api) RegisterColumnConvert(name string, definition string) {
	ctx.columnConverter[name] = definition
}

func (ctx *Api) UnregisterConverter(name string) {
	delete(ctx.converters, name)
}

func (ctx *Api) Select(target IModel, where string, limit, offset int, args ...map[string]interface{}) ([]map[string]interface{}, error) {
	var res []map[string]interface{}
	query := ctx.generateSelect(target, where, limit, offset)
	if args != nil && len(args) > 0 {
		query = ctx.replaceParameter(query, args[0])
	}

	if !ctx.connection.IsConnected() {
		ctx.connection.Connect(50)
	}
	rows, err := ctx.connection.GetInstance().Query(query)
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
		if strings.Contains(columnName, "->") {
			tmpColumnInfos := strings.Split(columnName, "->")
			if len(tmpColumnInfos) < 2 {
				panic(fmt.Sprintf("column definition is wrong %v", columnName))
			}
			for key, def := range ctx.columnConverter {
				if key == tmpColumnInfos[1] {
					columnName = strings.ReplaceAll(def, "$column", tmpColumnInfos[0])
					if len(infos.Alias) < 1 {
						if strings.Contains(tmpColumnInfos[0], ".") {
							infos.Alias = strings.Split(tmpColumnInfos[0], ".")[1]
						} else {
							infos.Alias = tmpColumnInfos[0]
						}
					}
					break
				}
			}
		}
		if len(infos.Alias) < 1 {
			if strings.Contains(columnName, ".") {
				infos.Alias = strings.Split(columnName, ".")[1]
			} else {
				infos.Alias = columnName
			}
		}

		columnName += fmt.Sprintf(" as \"%v\"", infos.Alias)

		if counter > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(columnName)
		counter++
	}

	for idx := range sources {
		buf.WriteString(" ")
		buf.WriteString(types[idx])
		buf.WriteString(" ")
		buf.WriteString(sources[idx])
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
				switch v := scanResult[idx].(type) {
				case []uint8:
					if f.Type.Kind() == reflect.String {
						elem[info.FieldName] = string(v)
					}
					break
				default:
					elem[info.FieldName] = scanResult[idx]
				}
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

func (ctx *Api) replaceParameter(query string, args map[string]interface{}) string {
	for paramName, paramValue := range args {
		if !strings.Contains(query, paramName) {
			continue
		}
		query = strings.ReplaceAll(query, ":"+paramName, toSQLString(paramValue))
	}
	return query
}

func toSQLString(value interface{}) string {
	switch v := value.(type) {
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case *bool:
		if *v {
			return "true"
		} else {
			return "false"
		}
	case int:
		return strconv.FormatInt(int64(v), 10)
	case *int:
		return strconv.FormatInt(int64(*v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case *int8:
		return strconv.FormatInt(int64(*v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case *int16:
		return strconv.FormatInt(int64(*v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case *int32:
		return strconv.FormatInt(int64(*v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case *int64:
		return strconv.FormatInt(*v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case *uint:
		return strconv.FormatUint(uint64(*v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case *uint8:
		return strconv.FormatUint(uint64(*v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case *uint16:
		return strconv.FormatUint(uint64(*v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case *uint32:
		return strconv.FormatUint(uint64(*v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case *uint64:
		return strconv.FormatUint(*v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case *float32:
		return strconv.FormatFloat(float64(*v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case *float64:
		return strconv.FormatFloat(*v, 'f', -1, 64)
	case complex64:
		return strconv.FormatComplex(complex128(v), 'f', -1, 64)
	case *complex64:
		return strconv.FormatComplex(complex128(*v), 'f', -1, 128)
	case complex128:
		return strconv.FormatComplex(v, 'f', -1, 64)
	case *complex128:
		return strconv.FormatComplex(*v, 'f', -1, 64)
	case string:
		return fmt.Sprintf("'%v'", removeSqlInjections(v))
	case *string:
		return fmt.Sprintf("'%v'", removeSqlInjections(*v))
	case time.Time:
		return fmt.Sprintf("cast('%v' as timestamp)", v.Format(time.RFC3339))
	case *time.Time:
		return fmt.Sprintf("cast('%v' as timestamp)", (*v).Format(time.RFC3339))
	case []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64,
		[]complex64, []complex128, []string, []time.Time:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(v))
	case *[]int:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]int8:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]int16:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]int32:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]int64:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]uint:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]uint8:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]uint16:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]uint32:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]uint64:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]float32:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]float64:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]complex64:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]complex128:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]string:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	case *[]time.Time:
		return fmt.Sprintf("ARRAY[%v]", buildArrayContent(*v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func arrayInt(idsConverted []int) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(int64(id), 10))
	}
	return buf.String()
}

func arrayInt8(idsConverted []int8) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(int64(id), 10))
	}
	return buf.String()
}

func arrayInt16(idsConverted []int16) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(int64(id), 10))
	}
	return buf.String()
}

func arrayInt32(idsConverted []int32) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(int64(id), 10))
	}
	return buf.String()
}

func arrayInt64(idsConverted []int64) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(id, 10))
	}
	return buf.String()
}

func arrayUint(idsConverted []uint) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return buf.String()
}

func arrayUint8(idsConverted []uint8) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return buf.String()
}

func arrayUint16(idsConverted []uint16) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return buf.String()
}

func arrayUint32(idsConverted []uint32) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return buf.String()
}

func arrayUint64(idsConverted []uint64) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatUint(id, 10))
	}
	return buf.String()
}

func arrayFloat32(idsConverted []float32) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatFloat(float64(id), 'f', -1, 64))
	}
	return buf.String()
}

func arrayFloat64(idsConverted []float64) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatFloat(id, 'f', -1, 64))
	}
	return buf.String()
}

func arrayComplex64(idsConverted []complex64) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatComplex(complex128(id), 'f', -1, 64))
	}
	return buf.String()
}

func arrayComplex128(idsConverted []complex128) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatComplex(id, 'f', -1, 128))
	}
	return buf.String()
}

func arrayString(idsConverted []string) string {
	buf := bytes.NewBuffer([]byte{})
	for idx, id := range idsConverted {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("'")
		buf.WriteString(removeSqlInjections(id))
		buf.WriteString("'")
	}
	return buf.String()
}

func buildArrayContent(values interface{}) string {
	t := reflect.TypeOf(values)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Slice {
		return "ARRAY[]"
	}

	buf := bytes.NewBuffer([]byte{})
	switch converted := values.(type) {
	case []bool:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatBool(id))
		}
		break
	case []int:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatInt(int64(id), 10))
		}
		break
	case []int8:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatInt(int64(id), 10))
		}
		break
	case []int16:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatInt(int64(id), 10))
		}
		break
	case []int32:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatInt(int64(id), 10))
		}
		break
	case []int64:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatInt(id, 10))
		}
		break
	case []uint:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatUint(uint64(id), 10))
		}
		break
	case []uint8:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatUint(uint64(id), 10))
		}
		break
	case []uint16:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatUint(uint64(id), 10))
		}
		break
	case []uint32:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatUint(uint64(id), 10))
		}
		break
	case []uint64:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatUint(id, 10))
		}
		break
	case []float32:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatFloat(float64(id), 'f', -1, 32))
		}
		break
	case []float64:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatFloat(id, 'f', -1, 64))
		}
		break
	case []complex64:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatComplex(complex128(id), 'f', -1, 64))
		}
		break
	case []complex128:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(strconv.FormatComplex(id, 'f', -1, 128))
		}
		break
	case []string:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("'%v'", removeSqlInjections(id)))
		}
		break
	case []time.Time:
		for idx, id := range converted {
			if idx > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("cast('%v' as timestamp)", id.Format(time.RFC3339)))
		}
		break
	}
	return buf.String()
}

func removeSqlInjections(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
