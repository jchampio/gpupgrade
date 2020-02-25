package hub

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/hashicorp/go-multierror"

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

func UpgradeMirrors(stateDir string, conf *InitializeConfig, targetRunner GreenplumRunner) (err error) {
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

	return runAddMirrors(targetRunner, path)
}
