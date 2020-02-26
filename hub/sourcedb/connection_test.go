package sourcedb

import "database/sql"

func (d *database) SetConnection(connection *sql.DB) {
	d.connection = connection
}
