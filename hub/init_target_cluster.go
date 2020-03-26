package hub

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/step"
)

func (s *Server) GenerateInitsystemConfig() error {
	sourceDBConn := db.NewDBConn("localhost", int(s.Source.MasterPort()), "template1")
	return s.writeConf(sourceDBConn)
}

func (s *Server) initsystemConfPath() string {
	return filepath.Join(s.StateDir, "gpinitsystem_config")
}

func (s *Server) writeConf(sourceDBConn *dbconn.DBConn) error {
	err := sourceDBConn.Connect(1)
	if err != nil {
		return errors.Wrap(err, "could not connect to database")
	}
	defer sourceDBConn.Close()

	gpinitsystemConfig, err := CreateInitialInitsystemConfig(s.TargetInitializeConfig.Master.DataDir)
	if err != nil {
		return err
	}

	gpinitsystemConfig, err = GetCheckpointSegmentsAndEncoding(gpinitsystemConfig, sourceDBConn)
	if err != nil {
		return err
	}

	gpinitsystemConfig, err = WriteSegmentArray(gpinitsystemConfig, s.TargetInitializeConfig)
	if err != nil {
		return xerrors.Errorf("generating segment array: %w", err)
	}

	return WriteInitsystemFile(gpinitsystemConfig, s.initsystemConfPath())
}

func (s *Server) CreateTargetCluster(stream step.OutStreams) error {
	err := s.InitTargetCluster(stream)
	if err != nil {
		return err
	}

	conn := db.NewDBConn("localhost", s.TargetInitializeConfig.Master.Port, "template1")
	defer conn.Close()

	s.Target, err = greenplum.ClusterFromDB(conn, s.Target.BinDir)
	if err != nil {
		return errors.Wrap(err, "could not retrieve target configuration")
	}

	if err := s.SaveConfig(); err != nil {
		return err
	}

	return nil
}

func (s *Server) InitTargetCluster(stream step.OutStreams) error {
	return RunInitsystemForTargetCluster(stream, s.Target, s.initsystemConfPath())
}

func GetCheckpointSegmentsAndEncoding(gpinitsystemConfig []string, dbConnector *dbconn.DBConn) ([]string, error) {
	checkpointSegments, err := dbconn.SelectString(dbConnector, "SELECT current_setting('checkpoint_segments') AS string")
	if err != nil {
		return gpinitsystemConfig, errors.Wrap(err, "Could not retrieve checkpoint segments")
	}
	encoding, err := dbconn.SelectString(dbConnector, "SELECT current_setting('server_encoding') AS string")
	if err != nil {
		return gpinitsystemConfig, errors.Wrap(err, "Could not retrieve server encoding")
	}
	gpinitsystemConfig = append(gpinitsystemConfig,
		fmt.Sprintf("CHECK_POINT_SEGMENTS=%s", checkpointSegments),
		fmt.Sprintf("ENCODING=%s", encoding))
	return gpinitsystemConfig, nil
}

func CreateInitialInitsystemConfig(targetMasterDataDir string) ([]string, error) {
	gpinitsystemConfig := []string{`ARRAY_NAME="gp_upgrade cluster"`}

	segPrefix, err := GetMasterSegPrefix(targetMasterDataDir)
	if err != nil {
		return gpinitsystemConfig, errors.Wrap(err, "Could not get master segment prefix")
	}

	gpinitsystemConfig = append(gpinitsystemConfig, "SEG_PREFIX="+segPrefix, "TRUSTED_SHELL=ssh")

	return gpinitsystemConfig, nil
}

func WriteInitsystemFile(gpinitsystemConfig []string, gpinitsystemFilepath string) error {
	gpinitsystemContents := []byte(strings.Join(gpinitsystemConfig, "\n"))

	err := ioutil.WriteFile(gpinitsystemFilepath, gpinitsystemContents, 0644)
	if err != nil {
		return errors.Wrap(err, "Could not write gpinitsystem_config file")
	}
	return nil
}

func WriteSegmentArray(config []string, targetInitializeConfig InitializeConfig) ([]string, error) {
	//Partition segments by host in order to correctly assign ports.
	if targetInitializeConfig.Master == (greenplum.SegConfig{}) {
		return nil, errors.New("source cluster contains no master segment")
	}

	master := targetInitializeConfig.Master
	config = append(config,
		fmt.Sprintf("QD_PRIMARY_ARRAY=%s~%d~%s~%d~%d",
			master.Hostname,
			master.Port,
			master.DataDir,
			master.DbID,
			master.ContentID,
		),
	)

	config = append(config, "declare -a PRIMARY_ARRAY=(")
	for _, segment := range targetInitializeConfig.Primaries {
		config = append(config,
			fmt.Sprintf("\t%s~%d~%s~%d~%d",
				segment.Hostname,
				segment.Port,
				segment.DataDir,
				segment.DbID,
				segment.ContentID,
			),
		)
	}
	config = append(config, ")")

	return config, nil
}

func RunInitsystemForTargetCluster(stream step.OutStreams, target *greenplum.Cluster, gpinitsystemFilepath string) error {
	gphome := filepath.Dir(path.Clean(target.BinDir)) //works around https://github.com/golang/go/issues/4837 in go10.4

	args := "-a -I " + gpinitsystemFilepath
	if target.Version.SemVer.Major < 7 {
		// For 6X we add --ignore-warnings to gpinitsystem to return 0 on
		// warnings and 1 on errors. 7X and later does this by default.
		args += " --ignore-warnings"
	}

	script := fmt.Sprintf("source %[1]s/greenplum_path.sh && %[1]s/bin/gpinitsystem %[2]s",
		gphome,
		args,
	)
	cmd := execCommand("bash", "-c", script)

	cmd.Stdout = stream.Stdout()
	cmd.Stderr = stream.Stderr()

	err := cmd.Run()
	if err != nil {
		return xerrors.Errorf("gpinitsystem: %w", err)
	}

	return nil
}

func GetMasterSegPrefix(datadir string) (string, error) {
	const masterContentID = "-1"

	base := path.Base(datadir)
	if !strings.HasSuffix(base, masterContentID) {
		return "", fmt.Errorf("path requires a master content identifier: '%s'", datadir)
	}

	segPrefix := strings.TrimSuffix(base, masterContentID)
	if segPrefix == "" {
		return "", fmt.Errorf("path has no segment prefix: '%s'", datadir)
	}
	return segPrefix, nil
}
