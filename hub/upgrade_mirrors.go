package hub

import (
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/utils"
)

func writeGpAddmirrorsConfig(conf *InitializeConfig, out io.Writer) error {
	for _, m := range conf.Mirrors {
		_, err := fmt.Fprintf(out, "%d|%s|%d|%s\n", m.ContentID, m.Hostname, m.Port, upgradeDataDir(m.DataDir)) // XXX this should go into the config
		if err != nil {
			return err
		}
	}
	return nil
}

func runAddMirrors(r GreenplumRunner, filepath string) error {
	return r.Run("gpaddmirrors",
		"-a",
		"-i", filepath,
	)
}

func waitForFTS(masterPort int) error {
	// TODO: pull this up, and especially test for search_path sanitization
	connURI := fmt.Sprintf("postgresql://localhost:%d/template1?gp_session_role=utility&search_path=", masterPort)
	db, err := sql.Open("pgx", connURI)
	if err != nil {
		return err
	}

	defer db.Close()

	for {
		rows, err := db.Query("SELECT gp_request_fts_probe_scan();")
		if err != nil {
			return xerrors.Errorf("requesting probe scan: %w", err)
		}

		if err := rows.Close(); err != nil {
			return xerrors.Errorf("closing probe scan results: %w", err)
		}

		err = func() error {
			rows, err = db.Query(`
				SELECT every(status = 'u')
					FROM gp_segment_configuration
					WHERE role = 'm'
			`)
			if err != nil {
				return xerrors.Errorf("querying mirror status: %w", err)
			}

			defer rows.Close() // XXX lost error

			for rows.Next() {
				var up bool
				if err := rows.Scan(&up); err != nil {
					return xerrors.Errorf("scanning mirror status: %w", err)
				}

				if up {
					return nil
				}
			}
			if err := rows.Err(); err != nil {
				return xerrors.Errorf("iterating mirror status: %w", err)
			}

			return nil
		}()
		if err != nil {
			return err
		}

		time.Sleep(time.Second)
	}
}

func UpgradeMirrors(stateDir string, masterPort int, conf *InitializeConfig, targetRunner GreenplumRunner) (err error) {
	path := filepath.Join(stateDir, "add_mirrors_config")
	// calling Close() on a file twice results in an error
	// only call Close() in the defer if we haven't yet tried to close it.
	fileClosed := false

	f, err := utils.System.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if !fileClosed {
			if cerr := f.Close(); cerr != nil {
				err = multierror.Append(err, cerr).ErrorOrNil()
			}
		}
	}()

	err = writeGpAddmirrorsConfig(conf, f)
	if err != nil {
		return err
	}

	err = f.Close()
	fileClosed = true
	// not unit tested because stubbing it properly
	// would require too many extra layers
	if err != nil {
		return err
	}

	err = runAddMirrors(targetRunner, path)
	if err != nil {
		return err
	}

	return waitForFTS(masterPort)
}
