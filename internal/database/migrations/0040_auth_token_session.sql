-- +goose Up
-- +goose StatementBegin
-- 会话身份：登录时生成一次，refresh 轮换保持不变；client_info 仅用于展示，
-- 不能再充当会话键（相同 UA 的独立登录必须是不同会话）。
ALTER TABLE auth_tokens ADD COLUMN session_id TEXT NOT NULL DEFAULT '';
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_auth_tokens_session_id ON auth_tokens(session_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_auth_tokens_session_id;
-- +goose StatementEnd

-- +goose StatementBegin
-- 必须先删索引再删列：SQLite 不允许 DROP 被索引引用的列。
-- DROP COLUMN 需要 SQLite 3.35+，modernc.org/sqlite 绑定的版本满足要求。
ALTER TABLE auth_tokens DROP COLUMN session_id;
-- +goose StatementEnd
