// The dump_config utility dumps the configuration of a running GPDB
// cluster into the specified <configPath> file.
// The GPDB cluster is identified by the $PGPORT environment variable.
// The usage is:
//
//     dump_config <binDir> <configPath>
//
// where <binDir> is what you want the configuration to contain for
// the binary location.
package main

import (
	"log"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	"github.com/greenplum-db/gpupgrade/utils"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("usage: %s <binDir> <configPath>", os.Args[0])
	}

	binDir := os.Args[1]
	configPath := os.Args[2]
	conn := dbconn.NewDBConnFromEnvironment("postgres")

	cluster, err := utils.ClusterFromDB(conn, binDir, configPath)
	if err != nil {
		log.Fatal(err)
	}

	err = cluster.Commit(utils.WriteJSONFile)
	if err != nil {
		log.Fatal(err)
	}
}
