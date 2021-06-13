package annotations

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
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

func TestStreamsCRUDSingle(t *testing.T) {
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
	readGot, err := svc.UpdateStream(ctx, idGen.ID(), u2)
	require.Nil(t, readGot)
	require.Equal(t, errStreamNotFound, err)

	// can update an existing stream with UpdateStream, changing both the name and description
	s3, err := svc.UpdateStream(ctx, s2.ID, u2)
	require.NoError(t, err)
	require.Equal(t, s2.ID, s3.ID)
	require.Equal(t, u2.Name, s3.Name)
	require.Equal(t, u2.Description, s3.Description)

	// getting a non-existant stream returns a not found error
	storedGot, err := svc.GetStream(ctx, idGen.ID())
	require.Nil(t, storedGot)
	require.Equal(t, errStreamNotFound, err)

	// getting an existing stream returns the stream without error
	storedGot, err = svc.GetStream(ctx, s3.ID)
	require.NoError(t, err)
	require.Equal(t, s3.Name, storedGot.Name)
	require.Equal(t, s3.Description, storedGot.Description)

	// deleting a non-existant stream returns not found error
	err = svc.DeleteStreamByID(ctx, idGen.ID())
	require.Equal(t, errStreamNotFound, err)

	// can delete an existing stream without error...
	err = svc.DeleteStreamByID(ctx, s1.ID)
	require.NoError(t, err)
	// ...and the stream really does get deleted
	storedGot, err = svc.GetStream(ctx, s1.ID)
	require.Nil(t, storedGot)
	require.Equal(t, err, errStreamNotFound)
}

func TestStreamsCRUDMany(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)

	ctx := context.Background()

	// populate the database with some streams for testing delete and select many
	combos := map[platform.ID][]string{
		*influxdbtesting.IDPtr(1): {"org1_s1", "org1_s2", "org1_s3", "org1_s4"},
		*influxdbtesting.IDPtr(2): {"org2_s1"},
		*influxdbtesting.IDPtr(3): {"org3_s1", "org3_s2"},
	}

	for orgID, streams := range combos {
		for _, s := range streams {
			_, err := svc.CreateOrUpdateStream(ctx, orgID, influxdb.Stream{
				Name: s,
			})
			require.NoError(t, err)
		}
	}

	// all streams can be listed for each org if passing an empty list
	for orgID, streams := range combos {
		got, err := svc.ListStreams(ctx, orgID, influxdb.StreamListFilter{
			StreamIncludes: []string{},
		})
		require.NoError(t, err)
		assertStreamNames(t, streams, got)
	}

	// can select specific streams and get only those for that org
	got, err := svc.ListStreams(ctx, *influxdbtesting.IDPtr(1), influxdb.StreamListFilter{
		StreamIncludes: []string{"org1_s1", "org1_s4", "org2_s1"},
	})
	require.NoError(t, err)
	assertStreamNames(t, []string{"org1_s1", "org1_s4"}, got)

	// can delete a single stream with DeleteStreams, but does not delete streams for other org
	err = svc.DeleteStreams(ctx, *influxdbtesting.IDPtr(1), influxdb.BasicStream{
		Names: []string{"org1_s1", "org2_s1"},
	})
	require.NoError(t, err)

	got, err = svc.ListStreams(ctx, *influxdbtesting.IDPtr(1), influxdb.StreamListFilter{
		StreamIncludes: []string{},
	})
	require.NoError(t, err)
	assertStreamNames(t, []string{"org1_s2", "org1_s3", "org1_s4"}, got)

	got, err = svc.ListStreams(ctx, *influxdbtesting.IDPtr(2), influxdb.StreamListFilter{
		StreamIncludes: []string{},
	})
	require.NoError(t, err)
	assertStreamNames(t, []string{"org2_s1"}, got)

	// trying to list a stream that doesn't exist returns and empty list
	got, err = svc.ListStreams(ctx, *influxdbtesting.IDPtr(2), influxdb.StreamListFilter{
		StreamIncludes: []string{"bad stream"},
	})
	require.NoError(t, err)
	require.Equal(t, []influxdb.StoredStream{}, got)

	// can delete all streams for orgs
	for orgID, streams := range combos {
		err = svc.DeleteStreams(ctx, orgID, influxdb.BasicStream{
			Names: streams,
		})
		require.NoError(t, err)

		got, err := svc.ListStreams(ctx, orgID, influxdb.StreamListFilter{
			StreamIncludes: []string{},
		})
		require.NoError(t, err)
		require.Equal(t, []influxdb.StoredStream{}, got)
	}
}

func TestCreateAndListAnnotations(t *testing.T) {
	t.Parallel()

	et1 := time.Now().UTC()
	st1 := et1.Add(-10 * time.Minute)

	et2 := et1.Add(-5 * time.Minute)
	st2 := et2.Add(-10 * time.Minute)

	svc, clean := newTestService(t)
	defer clean(t)

	orgID := *influxdbtesting.IDPtr(1)
	ctx := context.Background()

	s1 := influxdb.StoredAnnotation{
		OrgID:     orgID,
		StreamTag: "stream1",
		Summary:   "summary1",
		Message:   "message1",
		Stickers:  map[string]string{"stick1": "val1", "stick2": "val2"},
		Duration:  fmt.Sprintf("[%s, %s]", st1.Format(time.RFC3339Nano), et1.Format(time.RFC3339Nano)),
		Lower:     st1.Format(time.RFC3339Nano),
		Upper:     et1.Format(time.RFC3339Nano),
	}

	c1, err := s1.ToCreate()
	require.NoError(t, err)

	s2 := influxdb.StoredAnnotation{
		OrgID:     orgID,
		StreamTag: "stream2",
		Summary:   "summary2",
		Message:   "message2",
		Stickers:  map[string]string{"stick2": "val2", "stick3": "val3", "stick4": "val4"},
		Duration:  fmt.Sprintf("[%s, %s]", st2.Format(time.RFC3339Nano), et2.Format(time.RFC3339Nano)),
		Lower:     st2.Format(time.RFC3339Nano),
		Upper:     et2.Format(time.RFC3339Nano),
	}

	c2, err := s2.ToCreate()
	require.NoError(t, err)

	testCreates := []influxdb.AnnotationCreate{*c1, *c2}
	got, err := svc.CreateAnnotations(ctx, orgID, testCreates)
	require.NoError(t, err)
	require.ElementsMatch(t, testCreates, []influxdb.AnnotationCreate{got[0].AnnotationCreate, got[1].AnnotationCreate})

	t.Run("select with filters", func(t *testing.T) {
		earlierEt1 := et1.Add(-1 * time.Millisecond)
		laterSt2 := st2.Add(1 * time.Millisecond)
		impossibleTime := time.Time{}.Add(1 * time.Minute)

		tests := []struct {
			name string
			f    influxdb.AnnotationListFilter
			want []influxdb.StoredAnnotation
		}{
			{
				"time filter is inclusive",
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &st2,
						EndTime:   &et1,
					},
				},
				[]influxdb.StoredAnnotation{s1, s2},
			},
			{
				"end time will filter out annotations",
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &st2,
						EndTime:   &earlierEt1,
					},
				},
				[]influxdb.StoredAnnotation{s2},
			},
			{
				"start time will filter out annotations",
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &laterSt2,
						EndTime:   &et1,
					},
				},
				[]influxdb.StoredAnnotation{s1},
			},
			{
				"time can filter out all annotations",
				influxdb.AnnotationListFilter{
					BasicFilter: influxdb.BasicFilter{
						StartTime: &impossibleTime,
						EndTime:   &impossibleTime,
					},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"can filter by stickers - one sticker matches one",
				influxdb.AnnotationListFilter{
					StickerIncludes: map[string]string{"stick1": "val1"},
				},
				[]influxdb.StoredAnnotation{s1},
			},
			{
				"can filter by stickers - one sticker matches multiple",
				influxdb.AnnotationListFilter{
					StickerIncludes: map[string]string{"stick2": "val2"},
				},
				[]influxdb.StoredAnnotation{s1, s2},
			},
			{
				"can filter by stickers - matching key but wrong value",
				influxdb.AnnotationListFilter{
					StickerIncludes: map[string]string{"stick2": "val3"},
				},
				[]influxdb.StoredAnnotation{},
			},
			{
				"can filter by stream - matches one",
				influxdb.AnnotationListFilter{
					StreamIncludes: []string{"stream2"},
				},
				[]influxdb.StoredAnnotation{s2},
			},
			{
				"can filter by stream - no match",
				influxdb.AnnotationListFilter{
					StreamIncludes: []string{"badStream"},
				},
				[]influxdb.StoredAnnotation{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.f.Validate(time.Now)
				listed, err := svc.ListAnnotations(ctx, orgID, tt.f)
				require.NoError(t, err)
				assertStoredAnnotations(t, tt.want, listed)
			})
		}
	})
}

func TestCascadeStreamDelete(t *testing.T) {

}

func assertStoredAnnotations(t *testing.T, want []influxdb.StoredAnnotation, got []influxdb.StoredAnnotation) {
	t.Helper()

	require.Equal(t, len(want), len(got))

	sort.Slice(want, func(i, j int) bool {
		return want[i].StreamTag < want[j].StreamTag
	})

	sort.Slice(got, func(i, j int) bool {
		return got[i].StreamTag < got[j].StreamTag
	})

	for idx, w := range want {
		w.ID = got[idx].ID
		w.StreamID = got[idx].StreamID
		require.Equal(t, w, got[idx])
	}
}

func assertStreamNames(t *testing.T, want []string, got []influxdb.StoredStream) {
	t.Helper()

	storedNames := make([]string, len(got))
	for i, s := range got {
		storedNames[i] = s.Name
	}

	require.ElementsMatch(t, want, storedNames)
}

func newTestService(t *testing.T) (*Service, func(t *testing.T)) {
	t.Helper()

	store, clean := sqlite.NewTestStore(t)
	ctx := context.Background()

	sqliteMigrator := sqlite.NewMigrator(store, zap.NewNop())
	err := sqliteMigrator.Up(ctx, migrations.All)
	require.NoError(t, err)

	svc := NewService(zap.NewNop(), store)

	return svc, clean
}
