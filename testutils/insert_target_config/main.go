// The insert_target_config utility inserts the configuration of the target GPDB
// cluster into the specified <configPath> file.
// The GPDB cluster is identified by the $PGPORT environment variable.
// The usage is:
//
//     insert_target_config <binDir> <configPath>
//
// where <binDir> is what you want the configuration to contain for
// the binary location.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/user"
	"strconv"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/hub"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("usage: %s <binDir> <configPath>", os.Args[0])
	}

	binDir := os.Args[1]
	configPath := os.Args[2]
	// open the file to overwrite the existing contents
	file, err := os.OpenFile(configPath, os.O_RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	var config hub.Config
	// load the contents of the file to config
	err = config.Load(file)
	if err != nil {
		log.Fatal(err)
	}

	// populate the contents of target cluster to config
	db := DBFromEnv("postgres")
	defer db.Close()

	config.Target, err = greenplum.ClusterFromDB(db, binDir)
	config.TargetInitializeConfig, err = hub.AssignDatadirsAndPorts(config.Source, []int{})
	if err != nil {
		log.Fatal(err)
	}

	// go to the start of the file
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	// truncate the file so that the new config could be
	// written
	err = file.Truncate(0)
	if err != nil {
		log.Fatal(err)
	}

	// write the new contents to the file
	err = config.Save(file)
	if err != nil {
		log.Fatal(err)
	}
}

func DBFromEnv(dbname string) *sql.DB {
	if dbname == "" {
		log.Fatal("no database provided")
	}

	username := os.Getenv("PGUSER")
	if username == "" {
		currentUser, _ := user.Current()
		username = currentUser.Username
	}

	host := os.Getenv("PGHOST")
	if host == "" {
		host, _ = os.Hostname()
	}

	port, err := strconv.Atoi(os.Getenv("PGPORT"))
	if err != nil {
		port = 5432
	}

	postgresURL := url.URL{
		Scheme:   "postgresql",
		User:     url.User(username),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + dbname,
		RawQuery: "gp_session_role=utility&search_path=",
	}

	db, err := sql.Open("pgx", postgresURL.String())
	if err != nil {
		log.Fatalf("connecting to cluster: %v", err)
	}

	return db
}
