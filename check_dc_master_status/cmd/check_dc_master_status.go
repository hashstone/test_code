package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"test_code/check_dc_master_status/db_access"
)

var (
	dcHost = flag.String("dcHost", "127.0.0.1", "the host of dc")
	dcPort = flag.Int("dcPort", 30447, "the port of dc")
	drHost = flag.String("drHost", "127.0.0.1", "the host of dr")
	drPort = flag.Int("drPort", 30447, "the port of dr")
	user   = flag.String("user", "root", "the user of mysql")
	passwd = flag.String("passwd", "", "the password of mysql")
)

func sortOffsets(s *dbaccess.MasterStatus) []string {
	ss := []string{}
	for _, item := range s.Items {
		offsets := strings.Split(item.ExecutedGtid, ",")
		ss = append(ss, offsets...)
	}
	sort.Strings(ss)
	return ss
}

func checkStatusEqual(l *dbaccess.MasterStatus, r *dbaccess.MasterStatus) bool {
	if l == nil || r == nil {
		log.Println("status is nil")
		return false
	}

	lStatusSet := sortOffsets(l)
	rStatusSet := sortOffsets(r)

	log.Printf("dcStatus[%v]: %v\n", len(lStatusSet), lStatusSet)
	log.Printf("drStatus[%v]: %v\n", len(rStatusSet), rStatusSet)

	if len(lStatusSet) != len(rStatusSet) {
		log.Println("length not match")
		return false
	}
	for i := range lStatusSet {
		if lStatusSet[i] != rStatusSet[i] {
			return false
		}
	}
	return true
}

func main() {
	flag.Parse()

	fmt.Printf("")
	dcAccess, err := dbaccess.NewAuth(&dbaccess.AuthConfig{
		Host:   *dcHost,
		Port:   *dcPort,
		User:   *user,
		Passwd: *passwd,
	})
	if err != nil {
		log.Fatalf("dc dbaccess.NewAuth fail:%v", err)
	}

	drAccess, err := dbaccess.NewAuth(&dbaccess.AuthConfig{
		Host:   *drHost,
		Port:   *drPort,
		User:   *user,
		Passwd: *passwd,
	})
	if err != nil {
		log.Fatalf("dr dbaccess.NewAuth fail:%v", err)
	}

	dcStatus, err := dcAccess.GetStatus()
	if err != nil {
		log.Fatalf("dc drAccess.GetStatus fail:%v", err)
	}
	drStatus, err := drAccess.GetStatus()
	if err != nil {
		log.Fatalf("dr drAccess.GetStatus fail:%v", err)
	}

	log.Println("dc status: ", dcStatus)
	log.Println("dr status: ", drStatus)

	if checkStatusEqual(dcStatus, drStatus) {
		log.Println("dr ok")
	} else {
		log.Println("dr fail")
	}
}
