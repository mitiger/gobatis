package gobatis

import (
	"database/sql"
	"errors"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
)

var (
	ErrParseRowFailed = errors.New("parse row failed with not support type.")
	ErrConverType     = errors.New("conver to an error type.")
)

func ScanV2(rows *sql.Rows, out interface{}) (err error) {
	defer func() {
		if x := recover(); x != nil {
			debug.PrintStack()
			err = ErrConverType
		}
	}()

	outtype := reflect.TypeOf(out)
	if outtype.Kind() != reflect.Ptr {
		err = ErrParseRowFailed
		return
	}

	ov := reflect.Indirect(reflect.ValueOf(out))
	ot := ov.Type()
	for rows.Next() {
		rmap, _ := rowstoMap2(rows)
		switch ot.Kind() {

		case reflect.Struct:
			out2Struct(rmap, ov)

		case reflect.Slice:
			out2Slice(rmap, reflect.ValueOf(out))

		default:
			for _, v := range rmap {
				err = out2Variable(v, ov)
			}
		}
	}
	return
}

func out2Slice(rmap map[string]string, ov reflect.Value) {

	elev := ov.Elem()
	elet := elev.Type().Elem()
	switch elet.Kind() {

	case reflect.Struct:
		temp := reflect.New(elet).Elem()
		out2Struct(rmap, temp)
		elev.Set(reflect.Append(elev, temp))

	case reflect.Ptr:
		t := elet.Elem()
		switch t.Kind() {
		case reflect.Struct:
			temp := reflect.New(elet.Elem())
			out2Struct(rmap, temp.Elem())
			elev.Set(reflect.Append(elev, temp))
		default:
			for _, v := range rmap {
				temp := reflect.New(elet.Elem())
				out2Variable(v, temp.Elem())
				elev.Set(reflect.Append(elev, temp))
			}
		}

	default:
		for _, v := range rmap {
			temp := reflect.New(elet).Elem()
			out2Variable(v, temp)
			elev.Set(reflect.Append(elev, temp))
		}
	}

}

func out2Struct(rmap map[string]string, ov reflect.Value) (err error) {
	num := ov.Type().NumField()
	for i := 0; i < num; i++ {
		fv := ov.Field(i)
		if !fv.CanSet() {
			continue
		}
		ft := ov.Type().Field(i)
		fieldname := strings.ToLower(ft.Name)
		v := rmap[fieldname]
		switch fv.Type().Kind() {
		case reflect.Struct:
			out2Struct(rmap, fv)
		default:
			out2Variable(v, fv)
		}
	}
	return
}

func out2Variable(in string, ov reflect.Value) (err error) {
	ot := ov.Type()
	switch ot.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32:
		i, _ := strconv.ParseInt(in, 10, 64)
		ov.SetInt(i)

	case reflect.String:
		ov.SetString(in)

	case reflect.Bool:
		i, _ := strconv.Atoi(in)
		if i == 0 {
			ov.Set(reflect.ValueOf(false))
		} else {
			ov.Set(reflect.ValueOf(true))
		}

	case reflect.Float32, reflect.Float64:
		i, _ := strconv.ParseFloat(in, 64)
		ov.SetFloat(i)

	default:
		err = ErrParseRowFailed
	}
	return
}

func rowstoMap2(rows *sql.Rows) (rm map[string]string, err error) {
	columns, err := rows.Columns()
	vs := make([]interface{}, len(columns))
	for i := 0; i < len(vs); i++ {
		var cell interface{}
		vs[i] = &cell
	}
	err = rows.Scan(vs...)
	if err != nil {
		return
	}
	rm = make(map[string]string)
	for idx, column := range columns {
		column = strings.ToLower(column)
		cell := vs[idx].(*interface{})
		cv := reflect.Indirect(reflect.ValueOf(cell)).Interface()
		if cv == nil {
			rm[column] = ""
			continue
		}
		ct := reflect.TypeOf(cv)
		switch ct.Kind() {
		case reflect.Int64:
			rm[column] = strconv.FormatInt(cv.(int64), 10)
		case reflect.Float64:
			rm[column] = strconv.FormatFloat(cv.(float64), 'f', -1, 64)
		case reflect.Float32:
			rm[column] = strconv.FormatFloat(cv.(float64), 'f', -1, 32)
		default:
			rm[column] = string(cv.([]byte))
		}
	}
	return
}

/*
func Scan(rows *sql.Rows, bean interface{}) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = ErrConverType
		}
	}()

	defer rows.Close()
	beantype := reflect.TypeOf(bean)
	if beantype.Kind() != reflect.Ptr {
		err = ErrParseRowFailed
		return
	}

	st := reflect.Indirect(reflect.ValueOf(bean)).Type()
	beanvalue := reflect.Indirect(reflect.ValueOf(bean))

	switch st.Kind() {
	case reflect.Slice:
		sv := reflect.ValueOf(bean).Elem()
		et := sv.Type().Elem()
		tmp := reflect.New(et).Elem()
		for rows.Next() {
			rm, err := rowstoMap(rows)
			if err != nil {
				continue
			}
			switch et.Kind() {

			case reflect.Int64:
				var cloumnvalue int64
				for _, v := range rm {
					cloumnvalue = v.(int64)
				}
				sv.Set(reflect.Append(sv, reflect.ValueOf(cloumnvalue)))

			case reflect.Int:
				var cloumnvalue int64
				for _, v := range rm {
					cloumnvalue = v.(int64)
				}
				sv.Set(reflect.Append(sv, reflect.ValueOf(int(cloumnvalue))))

			case reflect.String:
				var cloumnvalue interface{}
				for _, v := range rm {
					cloumnvalue = string(v.([]byte))
				}
				sv.Set(reflect.Append(sv, reflect.ValueOf(cloumnvalue)))

			case reflect.Float64:
				var cloumnvalue float64
				for _, v := range rm {
					cloumnvalue, _ = strconv.ParseFloat(string(v.([]byte)), 64)
				}
				sv.Set(reflect.Append(sv, reflect.ValueOf(cloumnvalue)))
			case reflect.Float32:
				var cloumnvalue float64
				for _, v := range rm {
					cloumnvalue, _ = strconv.ParseFloat(string(v.([]byte)), 64)
				}
				sv.Set(reflect.Append(sv, reflect.ValueOf(float32(cloumnvalue))))

			case reflect.Bool:
				var cloumnvalue interface{}
				for _, v := range rm {
					cloumnvalue = v
				}
				bv, _ := cloumnvalue.(int64)
				if bv == 1 {
					sv.Set(reflect.Append(sv, reflect.ValueOf(true)))
				} else {
					sv.Set(reflect.Append(sv, reflect.ValueOf(false)))
				}

			case reflect.Struct:
				scanStruct(rm, tmp)
				sv.Set(reflect.Append(sv, tmp))
			}

		}

	case reflect.Struct:
		rows.Next()
		var rm map[string]interface{}
		rm, err = rowstoMap(rows)
		if err != nil {
			return
		}
		err = scanStruct(rm, reflect.ValueOf(bean))

	case reflect.Int64:
		rows.Next()
		var rm map[string]interface{}
		rm, _ = rowstoMap(rows)
		var cloumnvalue interface{}
		for _, v := range rm {
			cloumnvalue = v
		}
		beanvalue.Set(reflect.ValueOf(cloumnvalue))

	case reflect.Int:
		rows.Next()
		var rm map[string]interface{}
		rm, _ = rowstoMap(rows)
		var cloumnvalue interface{}
		for _, v := range rm {
			cloumnvalue = v
		}

		beanvalue.Set(reflect.ValueOf(int(cloumnvalue.(int64))))

	case reflect.String:
		rows.Next()
		var rm map[string]interface{}
		rm, _ = rowstoMap(rows)
		var cloumnvalue interface{}
		for _, v := range rm {
			cloumnvalue = string(v.([]byte))
		}
		beanvalue.Set(reflect.ValueOf(cloumnvalue))

	case reflect.Float64:
		rows.Next()
		var rm map[string]interface{}
		rm, _ = rowstoMap(rows)
		var cloumnvalue float64
		for _, v := range rm {
			cloumnvalue, _ = strconv.ParseFloat(string(v.([]byte)), 64)
		}
		beanvalue.Set(reflect.ValueOf(cloumnvalue))

	case reflect.Float32:
		rows.Next()
		var rm map[string]interface{}
		rm, _ = rowstoMap(rows)
		var cloumnvalue float64
		for _, v := range rm {
			cloumnvalue, _ = strconv.ParseFloat(string(v.([]byte)), 64)
		}
		beanvalue.Set(reflect.ValueOf(float32(cloumnvalue)))

	case reflect.Bool:
		rows.Next()
		var rm map[string]interface{}
		rm, _ = rowstoMap(rows)
		var cloumnvalue interface{}
		for _, v := range rm {
			cloumnvalue = v
		}
		bv, _ := cloumnvalue.(int64)
		var b bool
		if bv == 1 {
			b = true
		} else {
			b = false
		}
		beanvalue.Set(reflect.ValueOf(b))
	default:
		err = ErrParseRowFailed
	}

	return
}

func rowstoMap(rows *sql.Rows) (rm map[string]interface{}, err error) {
	columns, err := rows.Columns()
	vs := make([]interface{}, len(columns))
	for i := 0; i < len(vs); i++ {
		var cell interface{}
		vs[i] = &cell
	}
	err = rows.Scan(vs...)
	if err != nil {
		return
	}
	rm = make(map[string]interface{})
	for idx, column := range columns {
		cell := vs[idx].(*interface{})
		cv := reflect.Indirect(reflect.ValueOf(cell)).Interface()
		rm[column] = cv
	}
	return
}

func scanStruct(rm map[string]interface{}, bean reflect.Value) (err error) {
	vv := reflect.Indirect(bean)
	num := vv.NumField()
	for i := 0; i < num; i++ {
		field := vv.Field(i)
		if !field.CanSet() {
			continue
		}

		kind := field.Kind()
		fieldname := strings.ToLower(vv.Type().Field(i).Name)
		v := rm[fieldname]
		if v == nil {
			continue
		}

		switch kind {
		case reflect.String:
			field.SetString(string(v.([]byte)))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			iv, _ := v.(int64)
			field.SetInt(iv)

		case reflect.Float32, reflect.Float64:
			fv, _ := strconv.ParseFloat(string(v.([]byte)), 64)
			field.SetFloat(fv)

		case reflect.Bool:
			bv, _ := v.(int64)
			if bv == 1 {
				field.SetBool(true)
			} else {
				field.SetBool(false)
			}

		}
	}
	return
}
*/
