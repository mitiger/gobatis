package gobatis

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func (this *Engine) LoadXmlFile(files ...string) (err error) {
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			continue
		}

		err = parseXml(data)
		if err != nil {
			continue
		}
	}
	return nil
}

func LoadXmlDir(dir string) (err error) {
	err = filepath.Walk(dir, wf)
	return
}

func wf(path string, info os.FileInfo, err error) error {
	if info == nil {
		return nil
	}

	if !info.IsDir() && strings.Contains(info.Name(), ".xml") {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		err = parseXml(data)
		if err != nil {
			return errors.New("parse xml file with error, in file: " + info.Name() + "-->" + err.Error()) //errors.New(ErrTemplateParse.Error() + " , exception file is:" + info.Name())
		}
	}
	return nil
}

func parseArgs(args string, in interface{}) (params []interface{}) {
	if args == "" || in == nil {
		return
	}

	ls := strings.Split(args, ",")
	size := len(ls)
	params = make([]interface{}, size)

	inmap := make(map[string]interface{})
	it := reflect.TypeOf(in)
	iv := reflect.ValueOf(in)
	switch it.Kind() {
	case reflect.Struct:
		inmap = struct2map(iv)
	case reflect.Map:
		inmap = map2map(iv)
	case reflect.Ptr:
		piv := iv.Elem()
		pit := piv.Type()
		switch pit.Kind() {
		case reflect.Struct:
			inmap = struct2map(piv)
		case reflect.Map:
			inmap = map2map(piv)
		}
	}

	for i, value := range ls {
		value = strings.Replace(value, "{{.", "", -1)
		value = strings.Replace(value, "}}", "", -1)
		params[i] = inmap[strings.ToLower(strings.TrimSpace(value))]
	}
	return
}

func struct2map(args reflect.Value) (m map[string]interface{}) {
	m = make(map[string]interface{})
	at := args.Type()
	num := at.NumField()
	for i := 0; i < num; i++ {
		k := strings.TrimSpace(at.Field(i).Name)
		v := args.Field(i).Interface()
		m[strings.ToLower(k)] = v
	}
	return

}

func map2map(args reflect.Value) (m map[string]interface{}) {
	m = make(map[string]interface{})
	keys := args.MapKeys()
	for _, key := range keys {
		value := args.MapIndex(key)
		vt := reflect.TypeOf(value.Interface())
		vv := reflect.ValueOf(value.Interface())
		ks := strings.TrimSpace(strings.ToLower(key.String()))
		switch vt.Kind() {
		case reflect.Struct:
			sm := struct2map(vv)
			for sk, sv := range sm {
				m[ks+"."+sk] = sv
			}
		case reflect.Map:
			mm := map2map(vv)
			for sk, sv := range mm {
				m[ks+"."+sk] = sv
			}
		default:
			m[ks] = value.Interface()
		}
	}
	return
}

type Sql struct {
	Id    string `xml:"id,attr"`
	Query string `xml:"Query"`
	Args  string `xml: "Args"`
}

type Mapper struct {
	Namespace string `xml:"namespace,attr"`
	Sql       []Sql  `xml:"Sql"`
}

func parseXml(data []byte) (err error) {
	v := Mapper{}
	err = xml.Unmarshal(data, &v)
	if err != nil {
		return
	}
	for _, sql := range v.Sql {
		ps := new(Sql)
		ps.Id = sql.Id
		ps.Query = sql.Query
		ps.Args = sql.Args
		key := v.Namespace + "." + sql.Id
		sqls[key] = ps
	}
	return
}
