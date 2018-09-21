package dbaccess

type StatusItem struct {
	File         string
	Position     int64
	BinlogDB     string
	IgnoreDB     string
	ExecutedGtid string
}

// MasterStatus contains the information of master
type MasterStatus struct {
	Items []StatusItem
}

type authDataAccess interface {
	close()
	isConnOK() error
	queryMasterStatus() (*MasterStatus, error)
}
