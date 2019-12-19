package services

import (
	"reflect"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	"github.com/greenplum-db/gp-common-go-libs/cluster"

	"github.com/greenplum-db/gpupgrade/utils"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
)

func TestFillClusterConfigsSubstep(t *testing.T) {
	t.Run("it stores the given data in a json file", func(t *testing.T) {
		oldBinDir := "something"
		newBinDir := "somethingelse"
		oldPort := 54321
		stateDir := "somestatedir"

		fileNameStored := []string{}
		var contentsStored []interface{}

		jsonWriterFunc := func(fileName string, contents interface{}) error {
			fileNameStored = append(fileNameStored, fileName)
			contentsStored = append(contentsStored, contents)

			return nil
		}

		dbConn, sqlMock := testhelper.CreateMockDBConn()

		sqlMock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"versionstring"}).AddRow("(Greenplum Database 1.1.1)"))
		sqlMock.ExpectQuery("SELECT .*").WillReturnRows(sqlmock.NewRows([]string{"dbid", "contentid", "port", "hostname", "datadir"}).AddRow(0, 0, 0, "", ""))
		sqlMock.ExpectClose()

		utils.System.ReadFile = func(filename string) ([]byte, error) {
			return []byte("{}"), nil
		}
		_, _, err := fillClusterConfigsSubStep(nil, oldBinDir, newBinDir, oldPort, stateDir, true, jsonWriterFunc, dbConn)

		if err != nil {
			t.Errorf("got unexpected error, %v", err)
		}

		// Observe that we tried to store json
		expectedNumberOfCalls := 3

		if len(fileNameStored) != expectedNumberOfCalls {
			t.Fatalf("expected %d calls, got %d", expectedNumberOfCalls, len(fileNameStored))
		}

		if len(contentsStored) != expectedNumberOfCalls {
			t.Fatalf("expected %d calls, got %d", expectedNumberOfCalls, len(contentsStored))
		}

		sourceContent := contentsStored[0].(*utils.ClusterConfig)

		expectedSourceContent := &utils.ClusterConfig{
			SegConfigs: []cluster.SegConfig{{0, 0, 0, "", ""}},
			BinDir:     "something",
			Version:    dbconn.NewVersion("1.1.1"),
		}

		if !reflect.DeepEqual(sourceContent, expectedSourceContent) {
			t.Errorf("expected %+v, got %+v", expectedSourceContent, sourceContent)
		}

		targetContent := contentsStored[1].(*utils.ClusterConfig)

		expectedTargetContent := &utils.ClusterConfig{
			SegConfigs: []cluster.SegConfig{},
			BinDir:     "somethingelse",
		}

		if !reflect.DeepEqual(targetContent, expectedTargetContent) {
			t.Errorf("expected %#v, got %#v", expectedTargetContent, targetContent)
		}

		upgradeConfig := contentsStored[2].(*utils.UpgradeConfig)

		expectedUpgradeConfig := &utils.UpgradeConfig{
			UseLinkMode: true,
		}

		if !reflect.DeepEqual(upgradeConfig, expectedUpgradeConfig) {
			t.Errorf("expected %#v, got %#v", expectedUpgradeConfig, upgradeConfig)
		}
	})
}

// fillClusterConfigsSubStep
