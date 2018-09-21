package dbaccess

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrEmptyAuthConfigUser   = fmt.Errorf("empty AuthConfig user")
	ErrEmptyAuthConfigPasswd = fmt.Errorf("empty AuthConfig passwd")
	ErrEmptyAuthConfigHost   = fmt.Errorf("empty AuthConfig host")
	ErrEmptyAuthConfigDBName = fmt.Errorf("empty AuthConfig dbName")
	ErrEmptyAuthConfigTable  = fmt.Errorf("empty AuthConfig table")
	ErrEmptyUserKey          = fmt.Errorf("empty userKey")
	ErrBadUserKey            = fmt.Errorf("bad userkey")
	ErrBadActiveInfo         = fmt.Errorf("bad activeInfo")
	ErrBadAccessType         = fmt.Errorf("bad auth access type")
	ErrInactivation          = fmt.Errorf("user inactivation")
	ErrNotInActivationPeroid = fmt.Errorf("user not in activation period")
)

// AuthConfig contains the parameters to construct Auth
type AuthConfig struct {
	Host       string
	Port       int
	User       string
	Passwd     string
	DbName     string
	Table      string
	AccessType string
}

// Auth is the module to verify that a user whether have right to access the region or not
type Auth struct {
	config *AuthConfig
	access authDataAccess
}

func checkAuthConfig(config *AuthConfig) error {
	if config.User == "" {
		return ErrEmptyAuthConfigUser
	}
	if config.Passwd == "" {
		return ErrEmptyAuthConfigPasswd
	}
	if config.Host == "" {
		return ErrEmptyAuthConfigHost
	}
	return nil
}

// NewAuth constuct a New Auth
func NewAuth(config *AuthConfig) (*Auth, error) {
	err := checkAuthConfig(config)
	if err != nil {
		return nil, err
	}

	auth := &Auth{config: config}
	accessType := config.AccessType
	if accessType == "" {
		accessType = defaultDBType
	}
	switch accessType {
	case defaultDBType:
		access, err := newAuthDataAccessMYSQL(config)
		if err != nil {
			return nil, err
		}
		auth.access = access
	default:
		return nil, ErrBadAccessType
	}
	return auth, nil
}

// Close Auth
func (auth *Auth) Close() {
	auth.access.close()
}

// GetStatus get master status
func (auth *Auth) GetStatus() (*MasterStatus, error) {
	return auth.access.queryMasterStatus()
}
