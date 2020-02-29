package sourcedb

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetSegmentStatuses(t *testing.T) {
	t.Run("it returns segment statuses", func(t *testing.T) {
		connection, sqlmock, err := sqlmock.New()

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
}
