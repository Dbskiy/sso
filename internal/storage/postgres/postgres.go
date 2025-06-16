package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"sso/internal/domain/models"
	"sso/internal/storage"
)

type Storage struct {
	pool *pgxpool.Pool
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.postgres.New"
	pool, err := pgxpool.New(context.Background(), storagePath)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return &Storage{pool}, nil
}

func (s *Storage) Close() {
	s.pool.Close()
}

func (s *Storage) SaveUser(ctx context.Context, email string, passHash []byte) (int64, error) {
	const op = "storage.postgres.SaveUser"
	//TODO: подготовить запрос, если сервер не справляется с нагрузкой, когда регистрируется очень много пользователей
	var userID int64
	err := s.pool.QueryRow(ctx, "INSERT INTO users(email, pass_hash) VALUES ($1, $2) RETURNING id", email, passHash).Scan(&userID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgerrcode.UniqueViolation {
				return 0, fmt.Errorf("%s: %w", op, storage.ErrUserExists)
			}

		}

		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return userID, nil
}

func (s *Storage) User(ctx context.Context, email string) (models.User, error) {
	const op = "storage.postgres.User"

	var user models.User
	err := s.pool.QueryRow(ctx,
		"SELECT id, email, pass_hash FROM users WHERE email = $1", email).Scan(&user.ID, &user.Email, &user.PassHash)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.User{}, fmt.Errorf("%s: %w", op, storage.ErrUserNotFound)
		}
		return models.User{}, fmt.Errorf("%s: %w", op, err)
	}

	return user, nil
}

func (s *Storage) IsAdmin(ctx context.Context, userID int64) (bool, error) {
	const op = "storage.postgres.IsAdmin"

	var isAdmin bool
	err := s.pool.QueryRow(ctx, "SELECT is_admin FROM users WHERE id=$1", userID).Scan(&isAdmin)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("%s: %w", op, storage.ErrUserNotFound)
		}
		return false, fmt.Errorf("%s: %w", op, err)
	}

	return isAdmin, nil
}

func (s *Storage) App(ctx context.Context, appID int) (models.App, error) {
	const op = "storage.postgres.App"

	var app models.App
	err := s.pool.QueryRow(ctx,
		"SELECT id, name, secret FROM apps WHERE id=$1", appID).Scan(&app.ID, &app.Name, &app.Secret)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.App{}, fmt.Errorf("%s: %w", op, storage.ErrAppNotFound)
		}
		return models.App{}, fmt.Errorf("%s: %w", op, err)
	}

	return app, nil
}
