package annotations

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/sqlite/migrations"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

	s2, err := svc.CreateOrUpdateStream(ctx, orgID, update)
	require.NoError(t, err)
	require.Equal(t, stream.Name, s2.Name)
	require.Equal(t, u1.Description, s2.Description)
	require.Equal(t, s1.ID, s2.ID)

	//
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
