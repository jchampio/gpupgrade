package greenplum_test

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// mockDB returns a sql.DB backed by a Sqlmock controller that has been
// retrofitted with a deferrable Finish() method. Any errors are handled
// out-of-band using t.Fatalf().
func sqlmockDB(t *testing.T) (*sql.DB, *SqlmockEx) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("creating new sqlmock: %+v", err)
	}

	return db, &SqlmockEx{mock, t}
}

// SqlmockEx embeds sqlmock.Sqlmock and adds extra goodies.
type SqlmockEx struct {
	sqlmock.Sqlmock

	t *testing.T
}

// Finish is patterned on gomock's defer workflow. Call it to make sure the
// mock's expectations were met.
func (e *SqlmockEx) Finish() {
	if err := e.ExpectationsWereMet(); err != nil {
		e.t.Fatal(err)
	}
}
