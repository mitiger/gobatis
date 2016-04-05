package gobatis

import (
	"fmt"

	"bytes"
	"database/sql"
	"errors"

	"html/template"
	"reflect"
	"strings"
)

var sqls map[string]*Sql

var (
	ErrSqlNotFound   = errors.New("sql script not found.")
	ErrTemplateParse = errors.New("parse xml template failed.")
)

func init() {
	sqls = make(map[string]*Sql)
}

type Engine struct {
	db *sql.DB
}

func NewEngine(db *sql.DB) (engine *Engine) {
	engine = new(Engine)
	engine.db = db
	return
}

func (this *Engine) Begin() (tx *sql.Tx, err error) {
	tx, err = this.db.Begin()
	return
}

func (this *Engine) Rollback(tx *sql.Tx) (err error) {
	return tx.Rollback()

}

func (this *Engine) Commit(tx *sql.Tx) (err error) {
	return tx.Commit()

}

func (this *Engine) Exec(tx *sql.Tx, id string, in interface{}) (result sql.Result, err error) {
	return exec_v2(this.db, tx, id, in)
}

func exec_v1(tx *sql.Tx, id string, in interface{}) (result sql.Result, err error) {
	s := sqls[id]
	if s == nil {
		err = ErrSqlNotFound
		return
	}

	query, err := parseSql(s, in)
	if err != nil {
		return
	}

	var stmt *sql.Stmt
	stmt, err = tx.Prepare(query)
	if err != nil {
		return
	}
	defer stmt.Close()
	var params []interface{}
	params = parseArgs(s.Args, in)
	if params == nil {
		result, err = stmt.Exec()
	} else {
		result, err = stmt.Exec(params...)
	}
	return
}

func exec_v2(db *sql.DB, tx *sql.Tx, id string, in interface{}) (result sql.Result, err error) {
	s := sqls[id]
	if s == nil {
		err = ErrSqlNotFound
		return
	}
	query, err := parseSql(s, in)
	if err != nil {
		return
	}
	var params []interface{}
	params = parseArgs(s.Args, in)
	rquery := prepare(query, params)
	if tx == nil {
		result, err = db.Exec(rquery)
	} else {
		result, err = tx.Exec(rquery)
	}
	return
}

func (this *Engine) Query(tx *sql.Tx, id string, in interface{}, out interface{}) (err error) {
	return query_v2(this.db, tx, id, in, out)
}

func query_v1(db *sql.DB, tx *sql.Tx, id string, in interface{}, out interface{}) (err error) {
	s := sqls[id]
	if s == nil {
		err = ErrSqlNotFound
		return
	}

	query, err := parseSql(s, in)
	if err != nil {
		return
	}

	var stmt *sql.Stmt
	if tx == nil {
		stmt, err = db.Prepare(query)
	} else {
		stmt, err = tx.Prepare(query)
	}
	if err != nil {
		return
	}

	defer stmt.Close()
	var params []interface{}
	params = parseArgs(s.Args, in)

	var rows *sql.Rows
	if params == nil {
		rows, err = stmt.Query()
	} else {
		rows, err = stmt.Query(params...)
	}

	if err != nil {
		return
	}
	defer rows.Close()
	err = ScanV2(rows, out)
	return
}

func query_v2(db *sql.DB, tx *sql.Tx, id string, in interface{}, out interface{}) (err error) {
	s := sqls[id]
	if s == nil {
		err = ErrSqlNotFound
		return
	}

	query, err := parseSql(s, in)
	if err != nil {
		return
	}

	var params []interface{}
	params = parseArgs(s.Args, in)

	rquery := prepare(query, params)

	var rows *sql.Rows
	if tx == nil {
		rows, err = db.Query(rquery)
	} else {
		rows, err = tx.Query(rquery)
	}

	if err != nil {
		return
	}
	defer rows.Close()
	err = ScanV2(rows, out)
	return
}

//在这个函数中把sql中的占位符全部替换掉
func prepare(sql string, args []interface{}) (s string) {
	if !strings.Contains(sql, "?") {
		return sql
	}
	if args == nil || len(args) == 0 {
		return sql
	}

	ss := strings.Split(sql, "?")
	size := len(ss)

	for i := 0; i < size-1; i++ {
		arg := args[i]
		at := reflect.TypeOf(arg)
		var rs string
		switch at.Kind() {
		case reflect.String:
			rs = fmt.Sprintf("%q", arg)

		default:
			rs = fmt.Sprintf("%v", arg)
		}
		ss[i] += rs
		s += ss[i]
	}
	s = s + ss[size-1]
	return
}

func parseSql(sql *Sql, in interface{}) (query string, err error) {
	buffer := bytes.NewBufferString("")
	tmpl, err := template.New(sql.Id).Parse(sql.Query)
	if err != nil {
		return
	}
	err = tmpl.Execute(buffer, in)
	if err != nil {
		return
	}

	query = strings.TrimSpace(buffer.String())
	return
}
