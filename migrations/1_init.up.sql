CREATE TABLE IF NOT EXISTS users
(
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    pass_hash BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_email ON users (email); -- необязательно!!

CREATE TABLE IF NOT EXISTS apps
(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    secret TEXT NOT NULL UNIQUE
);