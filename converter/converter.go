package converter

import "reflect"

func SetDefaultValue(elem *map[string]interface{}, field reflect.StructField) {
	switch field.Type.Kind() {
	case reflect.Bool:
		(*elem)[field.Name] = false
		break
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		(*elem)[field.Name] = 0
		break
	case reflect.Float32, reflect.Float64:
		(*elem)[field.Name] = 0.0
		break
	case reflect.String:
		(*elem)[field.Name] = ""
		return
	default:
		(*elem)[field.Name] = nil
		return
	}
}
