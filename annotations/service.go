package annotations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

var (
	errAnnotationNotFound = &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  "annotation not found",
	}
	errStreamNotFound = &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  "stream not found",
	}
)

var _ influxdb.AnnotationService = (*Service)(nil)

type Service struct {
	store       *sqlite.SqlStore
	log         *zap.Logger
	idGenerator platform.IDGenerator
}

func NewService(logger *zap.Logger, store *sqlite.SqlStore) *Service {
	return &Service{
		store:       store,
		log:         logger,
		idGenerator: snowflake.NewIDGenerator(),
	}
}

// this will need some work. should be able to batch inserts using sqlx.
func (s *Service) CreateAnnotations(ctx context.Context, orgID platform.ID, creates []influxdb.AnnotationCreate) ([]influxdb.AnnotationEvent, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	events := make([]influxdb.AnnotationEvent, 0, len(creates))

	// store a unique list of stream names first
	streams := make(map[string]platform.ID)
	for _, c := range creates {
		streams[c.StreamTag] = platform.InvalidID()
	}

	// gonna need to get that into a list of ids. the keys are actually names. awesome!
	for name := range streams {
		// this might create a new stream, but probably just won't do anything other than a pointless database query
		s, err := s.CreateOrUpdateStream(ctx, orgID, influxdb.Stream{Name: name})
		if err != nil {
			return nil, err
		}

		// now we know the stream ID so set that
		streams[name] = s.ID
	}

	for _, c := range creates {
		a := influxdb.StoredAnnotation{
			ID:        s.idGenerator.ID(),
			OrgID:     orgID,
			StreamID:  streams[c.StreamTag],
			StreamTag: c.StreamTag,
			Summary:   c.Summary,
			Message:   c.Message,
			Stickers:  stickerMapToSlice(c.Stickers),
			Duration:  fmt.Sprintf("[%s, %s]", c.StartTime.Format(time.RFC3339Nano), c.EndTime.Format(time.RFC3339Nano)),
			Lower:     c.EndTime.Format(time.RFC3339Nano),
			Upper:     c.StartTime.Format(time.RFC3339Nano),
		}

		query := `
			INSERT INTO annotations(id, org_id, stream_id, stream_tag, summary, message, stickers, duration, lower, upper)
			VALUES (:id, :org_id, :stream_id, :stream_tag, :summary, :message, :stickers, :duration, :lower, :upper)
			RETURNING id, stream_tag, summary, message, stickers, lower, upper
		`

		stmt, err := s.store.DB.PrepareNamedContext(ctx, query)
		if err != nil {
			return nil, err
		}

		annEvent := influxdb.AnnotationEvent{}
		if err := stmt.Get(&annEvent, a); err != nil {
			return nil, err
		}

		events = append(events, annEvent)
	}

	return events, nil
}

func (s *Service) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) ([]influxdb.StoredAnnotation, error) {
	return nil, nil
}

// GetAnnotation checks to see if the authorizer on context has read access to the requested annotation
func (s *Service) GetAnnotation(ctx context.Context, id platform.ID) (*influxdb.StoredAnnotation, error) {
	return nil, nil
}

func (s *Service) DeleteAnnotations(ctx context.Context, orgID platform.ID, delete influxdb.AnnotationDeleteFilter) error {
	return nil
}

func (s *Service) DeleteAnnotation(ctx context.Context, id platform.ID) error {
	return nil
}

func (s *Service) UpdateAnnotation(ctx context.Context, id platform.ID, update influxdb.AnnotationCreate) (*influxdb.AnnotationEvent, error) {
	return nil, nil
}

// List streams list the stream named in the filter for the provided org.
func (s *Service) ListStreams(ctx context.Context, orgID platform.ID, filter influxdb.StreamListFilter) ([]influxdb.StoredStream, error) {
	query := `
		SELECT id, org_id, name, description, created_at, updated_at FROM streams
		WHERE org_id = ?`

	// if there are no stream names specified, the default behavior is to return all streams for the org
	if len(filter.StreamIncludes) == 0 {
		return s.listStreamsFromQueryAndArgs(ctx, query, orgID)
	}

	// note the leading space in this string, this is required!
	query += ` AND name IN (?)`
	query, args, err := sqlx.In(query, orgID, filter.StreamIncludes)
	if err != nil {
		return nil, err
	}
	query = s.store.DB.Rebind(query)

	return s.listStreamsFromQueryAndArgs(ctx, query, args...)
}

// listStreamsFromQueryAndArgs is a helper function for selecting a list of streams in a generalized way
func (s *Service) listStreamsFromQueryAndArgs(ctx context.Context, query string, args ...interface{}) ([]influxdb.StoredStream, error) {
	sts := []influxdb.StoredStream{}
	err := s.store.DB.SelectContext(ctx, &sts, query, args...)
	if err != nil {
		return nil, err
	}

	return sts, nil
}

func (s *Service) GetStream(ctx context.Context, id platform.ID) (*influxdb.StoredStream, error) {
	var st influxdb.StoredStream

	query := `
		SELECT id, org_id, name, description, created_at, updated_at
		FROM streams WHERE id = $1`

	if err := s.store.DB.GetContext(ctx, &st, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStreamNotFound
		}

		return nil, err
	}

	return &st, nil
}

// CreateOrUpdateStream creates a new stream, or updates the description to an existing stream.
// Inputs should be validated prior to call.
// Doesn't support updating a stream desctription to "". For that use the UpdateStream method.
func (s *Service) CreateOrUpdateStream(ctx context.Context, orgID platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	upsertQuery := `
		INSERT INTO streams(id, org_id, name, description, created_at, updated_at)
		VALUES(:id, :org_id, :name, :description, :created_at, :updated_at)
		ON CONFLICT(org_id, name) DO 
			UPDATE SET %s
			WHERE org_id = :org_id AND name = :name
		RETURNING id`

	setStr := "updated_at = :updated_at"
	if len(stream.Description) > 0 {
		setStr += ",description = :description"
	}
	upsertQuery = fmt.Sprintf(upsertQuery, setStr)

	stmt, err := s.store.DB.PrepareNamedContext(ctx, upsertQuery)
	if err != nil {
		return nil, err
	}

	nowTime := time.Now().UTC()
	newID := s.idGenerator.ID()

	var id platform.ID
	if err := stmt.GetContext(ctx, &id, influxdb.StoredStream{
		ID:          newID,
		OrgID:       orgID,
		Name:        stream.Name,
		Description: stream.Description,
		CreatedAt:   nowTime,
		UpdatedAt:   nowTime,
	}); err != nil {
		return nil, err
	}

	// do a separate query to get the resulting record from the db.
	// this is necessary because scanning strings from the db into time.Time types
	// from the RETURNING clause does not work, but scanning them with a SELECT does.
	return s.getReadStream(ctx, id)
}

// UpdateStream updates a stream name and or description. Inputs should be validated prior to call.
// Can be used to set a stream description to ""
func (s *Service) UpdateStream(ctx context.Context, id platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	query := `
		UPDATE streams
		SET name = :name, description = :description, updated_at = :updated_at
		WHERE id = :id`

	nowTime := time.Now().UTC()
	u := influxdb.ReadStream{
		ID:          id,
		Name:        stream.Name,
		Description: stream.Description,
		UpdatedAt:   nowTime,
	}

	rows, err := s.store.DB.NamedExecContext(ctx, query, u)
	if err != nil {
		return nil, err
	}

	n, err := rows.RowsAffected()
	if err != nil {
		return nil, err
	} else if n == 0 {
		return nil, errStreamNotFound
	}

	return s.getReadStream(ctx, id)
}

func (s *Service) DeleteStreams(ctx context.Context, orgID platform.ID, delete influxdb.BasicStream) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	query := `
		DELETE FROM streams
		WHERE org_id = ? AND name IN (?)`

	query, args, err := sqlx.In(query, orgID, delete.Names)
	if err != nil {
		return err
	}
	query = s.store.DB.Rebind(query)

	_, err = s.store.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return nil
}

// DeleteStreamByID deletes a single stream by ID
func (s *Service) DeleteStreamByID(ctx context.Context, id platform.ID) error {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	query := `
		DELETE FROM streams
		WHERE id = ?`

	res, err := s.store.DB.ExecContext(ctx, query, id.String())
	if err != nil {
		return err
	}

	r, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if r == 0 {
		return errStreamNotFound
	}

	return nil
}

// getReadStream is a helper which should only be called when the stream has been verified to exist
// via an update or insert.
func (s *Service) getReadStream(ctx context.Context, id platform.ID) (*influxdb.ReadStream, error) {
	query := `
		SELECT id, name, description, created_at, updated_at
		FROM streams WHERE id = ?`

	r := &influxdb.ReadStream{}
	if err := s.store.DB.GetContext(ctx, r, query, id); err != nil {
		return nil, err
	}

	return r, nil
}

func stickerMapToSlice(stickers map[string]string) []string {
	stickerSlice := []string{}

	for k, v := range stickers {
		stickerSlice = append(stickerSlice, k+"="+v)
	}

	return stickerSlice
}
