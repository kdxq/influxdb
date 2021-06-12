package annotations

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/sqlite/migrations"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	idGen = snowflake.NewIDGenerator()
)

func TestCreateOrUpdateStream(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)

	ctx := context.Background()
	orgID := *influxdbtesting.IDPtr(1)

	// create a stream
	stream := influxdb.Stream{
		Name:        "testName",
		Description: "original description",
	}

	s1, err := svc.CreateOrUpdateStream(ctx, orgID, stream)
	require.NoError(t, err)
	require.Equal(t, stream.Name, s1.Name)
	require.Equal(t, stream.Description, s1.Description)

	// update a stream with CreateOrUpdateStream - does not change the ID, but does change the description
	u1 := influxdb.Stream{
		Name:        "testName",
		Description: "updated description",
	}

	s2, err := svc.CreateOrUpdateStream(ctx, orgID, u1)
	require.NoError(t, err)
	require.Equal(t, stream.Name, s2.Name)
	require.Equal(t, u1.Description, s2.Description)
	require.Equal(t, s1.ID, s2.ID)

	u2 := influxdb.Stream{
		Name:        "otherName",
		Description: "other description",
	}

	// updating a non-existant stream with UpdateStream returns not found error
	got, err := svc.UpdateStream(ctx, idGen.ID(), u2)
	require.Nil(t, got)
	require.Equal(t, errStreamNotFound, err)

	// can update an existing stream with UpdateStream, changing both the name and description
	s3, err := svc.UpdateStream(ctx, s2.ID, u2)
	require.NoError(t, err)
	require.Equal(t, s2.ID, s3.ID)
	require.Equal(t, u2.Name, s3.Name)
	require.Equal(t, u2.Description, s3.Description)

}

func newTestService(t *testing.T) (*Service, func(t *testing.T)) {
	store, clean := sqlite.NewTestStore(t)
	ctx := context.Background()

	sqliteMigrator := sqlite.NewMigrator(store, zap.NewNop())
	err := sqliteMigrator.Up(ctx, migrations.All)
	require.NoError(t, err)

	svc := NewService(zap.NewNop(), store)

	return svc, clean
}
