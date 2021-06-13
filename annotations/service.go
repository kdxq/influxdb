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

func (s *Service) CreateAnnotations(ctx context.Context, orgID platform.ID, creates []influxdb.AnnotationCreate) ([]influxdb.AnnotationEvent, error) {
	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	// we need to create annotations with their stream_id set to whatever it needs to be
	// streams will need to be created if they don't exist

	// store a unique list of stream names first. the invalid ID is a placeholder for the real id
	streams := make(map[string]platform.ID)
	for _, c := range creates {
		streams[c.StreamTag] = platform.InvalidID()
	}

	tx, err := s.store.DB.BeginTxx(ctx, nil)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	for name := range streams {
		streamID, err := s.upsertStreamTx(ctx, tx, orgID, influxdb.Stream{Name: name})
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		streams[name] = *streamID
	}

	// now we can just create all the annotations
	createdEvents := make([]influxdb.AnnotationEvent, 0, len(creates))
	for _, create := range creates {
		// double check that we have a valid name for this stream tag
		streamID, ok := streams[create.StreamTag]
		if !ok {
			tx.Rollback()
			return nil, &ierrors.Error{
				Code: ierrors.EInternal,
				Msg:  fmt.Sprintf("unable to find id for stream %q", create.StreamTag),
			}
		}

		annotationInsertQuery := `
			INSERT INTO annotations(id, org_id, stream_id, name, summary, message, stickers, duration, lower, upper)
			VALUES(:id, :org_id, :stream_id, :name, :summary, :message, :stickers, :duration, :lower, :upper)
			RETURNING id, name, summary, message, stickers, lower, upper`

		stmt, err := tx.PrepareNamedContext(ctx, annotationInsertQuery)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		newID := s.idGenerator.ID()
		var res influxdb.StoredAnnotation
		if err := stmt.GetContext(ctx, &res, influxdb.StoredAnnotation{
			ID:        newID,
			OrgID:     orgID,
			StreamID:  streamID,
			StreamTag: create.StreamTag,
			Summary:   create.Summary,
			Message:   create.Message,
			Stickers:  create.Stickers,
			Duration:  fmt.Sprintf("[%s, %s]", create.StartTime.Format(time.RFC3339Nano), create.EndTime.Format(time.RFC3339Nano)),
			Lower:     create.StartTime.Format(time.RFC3339Nano),
			Upper:     create.EndTime.Format(time.RFC3339Nano),
		}); err != nil {
			tx.Rollback()
			return nil, err
		}

		st, err := time.Parse(time.RFC3339Nano, res.Lower)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		et, err := time.Parse(time.RFC3339Nano, res.Upper)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		createdEvents = append(createdEvents, influxdb.AnnotationEvent{
			ID: res.ID,
			AnnotationCreate: influxdb.AnnotationCreate{
				StreamTag: res.StreamTag,
				Summary:   res.Summary,
				Message:   res.Message,
				Stickers:  res.Stickers,
				EndTime:   &et,
				StartTime: &st,
			},
		})
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return createdEvents, nil
}

// upsertStreamTx is used for upserting a stream as part of an existing transaction.
// the caller is responsible for obtaining a lock on the database and managing the transaction
// lifecycle.
func (s *Service) upsertStreamTx(ctx context.Context, tx *sqlx.Tx, orgID platform.ID, stream influxdb.Stream) (*platform.ID, error) {
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

	stmt, err := tx.PrepareNamedContext(ctx, upsertQuery)
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

	return &id, nil
}

// ListAnnotations returns a list of annotations from the database matching the filter.
// For time range matching, sqlite is able to compare times with millisecond accuracy.
func (s *Service) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) ([]influxdb.StoredAnnotation, error) {
	query := `
		SELECT id, org_id, stream_id, name, summary, message, stickers, duration, lower, upper
		FROM ANNOTATIONS
		WHERE 
			lower >= ? AND upper <= ?`

	sf := filter.StartTime.Format(time.RFC3339Nano)
	ef := filter.EndTime.Format(time.RFC3339Nano)
	args := []interface{}{sf, ef}

	if len(filter.StreamIncludes) > 0 {
		query += ` AND name IN (?)`
		for _, s := range filter.StreamIncludes {
			args = append(args, s)
		}

		var err error
		query, args, err = sqlx.In(query, args...)
		if err != nil {
			return nil, err
		}
		query = s.store.DB.Rebind(query)
	}

	ans := []influxdb.StoredAnnotation{}
	if err := s.store.DB.SelectContext(ctx, &ans, query, args...); err != nil {
		return nil, err
	}

	return filterAnnsByStickers(ans, filter.StickerIncludes)
}

func filterAnnsByStickers(ans []influxdb.StoredAnnotation, sticks influxdb.AnnotationStickers) ([]influxdb.StoredAnnotation, error) {
	r := []influxdb.StoredAnnotation{}

	for _, a := range ans {
		exclude := false
		for key, val := range sticks {
			if a.Stickers[key] != val {
				exclude = true
				break
			}
		}
		if !exclude {
			r = append(r, a)
		}
	}

	return r, nil
}

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

	// a tx isn't strictly needed here, but using it for the query allows the upsert method
	// to be shared with other functions that need a transaction
	tx, err := s.store.DB.BeginTxx(ctx, nil)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	id, err := s.upsertStreamTx(ctx, tx, orgID, stream)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	// do a separate query to get the resulting record from the db.
	// this is necessary because scanning strings from the db into time.Time types
	// from the RETURNING clause does not work, but scanning them with a SELECT does.
	return s.getReadStream(ctx, *id)
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
