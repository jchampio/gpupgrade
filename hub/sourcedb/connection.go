package sourcedb

import (
	"database/sql"
	"errors"
	"fmt"
)

const connectionString = "postgresql://localhost:%d/template1?gp_session_role=utility&allow_system_table_mods=true&search_path="

func (d *database) Connect(port int) error {
	connection, err := sql.Open("pgx", fmt.Sprintf(connectionString, port))
	if err != nil {
		return err
	}

	d.connection = connection

	return nil
}

func (d *database) Connection() (*sql.DB, error) {
	if d.connection == nil {
		return nil, noConnectionError()
	}

	return d.connection, nil
}

func (d *database) Close() error {
	if d.connection == nil {
		return noConnectionError()
	}

	d.connection.Close()
	return nil
}

func noConnectionError() error {
	return errors.New("Database: no database connection established.")
}
