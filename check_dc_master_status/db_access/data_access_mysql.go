package dbaccess

import (
	"database/sql"
	"fmt"
	"log"
)

const (
	defaultDBType = "mysql"
)

type authDataAccessMYSQL struct {
	db *sql.DB

	querySQL string
}

func getDataSourceName(config *AuthConfig) string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=true",
		config.User,
		config.Passwd,
		config.Host,
		config.Port,
		config.DbName)
}

func newAuthDataAccessMYSQL(config *AuthConfig) (*authDataAccessMYSQL, error) {
	// 0. create db instance
	db, err := sql.Open(defaultDBType, getDataSourceName(config))
	if err != nil {
		return nil, err
	}
	access := authDataAccessMYSQL{
		db: db,
	}

	// 1. query sql
	access.querySQL = `show master status`
	return &access, nil
}

func (access *authDataAccessMYSQL) close() {
	access.db.Close()
}

func (access *authDataAccessMYSQL) isConnOK() error {
	return access.db.Ping()
}

func (access *authDataAccessMYSQL) queryMasterStatus() (*MasterStatus, error) {
	rows, err := access.db.Query(access.querySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var status MasterStatus
	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		if len(columns) != len(columnTypes) {
			return nil, fmt.Errorf("column length not match")
		}
		/*
			for i := range columns {
				fmt.Printf("name:%v type:%v\n", columns[i], columnTypes[i])
			}
		*/
		var item StatusItem
		err = rows.Scan(&item.File, &item.Position, &item.BinlogDB, &item.IgnoreDB, &item.ExecutedGtid)
		if err != nil {
			log.Fatal(err)
		}
		status.Items = append(status.Items, item)
	}
	return &status, rows.Err()
}
