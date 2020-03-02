package sourcedb

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"golang.org/x/xerrors"
)

func finishMock(mock sqlmock.Sqlmock, t *testing.T) {
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("%v", err)
	}
}

func TestGetSegmentStatuses(t *testing.T) {
	t.Run("it returns segment statuses", func(t *testing.T) {
		connection, sqlmock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("creating sqlmock: %+v", err)
		}
		defer finishMock(sqlmock, t)

		rows := sqlmock.
			NewRows([]string{"dbid", "is_up", "role", "preferred_role"}).
			AddRow("1", true, Mirror, Primary).
			AddRow("2", false, Primary, Mirror)

		query := `select dbid, status = .* as is_up, role, preferred_role
			from gp_segment_configuration`

		sqlmock.ExpectQuery(query).
			WithArgs(Up).
			WillReturnRows(rows)

		statuses, err := GetSegmentStatuses(connection)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(statuses) != 2 {
			t.Fatalf("got %d rows, expected 2 rows to be returned", len(statuses))
		}

		first := statuses[0]
		if first.DbID != 1 || first.IsUp != true || first.Role != Mirror || first.PreferredRole != Primary {
			t.Errorf("segment status not populated correctly: %+v", first)
		}

		second := statuses[1]
		if second.DbID != 2 || second.IsUp != false || second.Role != Primary || second.PreferredRole != Mirror {
			t.Errorf("segment status not populated correctly: %+v", second)
		}
	})

	t.Run("it returns an error if it fails to query for statuses", func(t *testing.T) {
		connection, sqlmock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("creating sqlmock: %+v", err)
		}
		defer finishMock(sqlmock, t)

		expected := errors.New("ahhhh")
		sqlmock.ExpectQuery(".*").WillReturnError(expected)

		_, err = GetSegmentStatuses(connection)
		if !xerrors.Is(err, expected) {
			t.Errorf("got error %#v, want %#v", err, expected)
		}
	})
}
