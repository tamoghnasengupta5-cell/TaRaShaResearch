-- Account records are deliberately separate from financial-research data.
-- Name and username are application-encrypted before they reach D1.
-- Passwords and security answers are stored only as salted, peppered PBKDF2 hashes.
create table if not exists consumer_users (
  id text primary key,
  username_lookup text not null unique,
  username_ciphertext text not null,
  name_ciphertext text not null,
  security_question_code text not null,
  password_hash text not null,
  security_answer_hash text not null,
  status text not null default 'active' check (status in ('active', 'disabled')),
  created_at integer not null,
  updated_at integer not null,
  last_login_at integer,
  password_reset_at integer
);

create unique index if not exists consumer_users_username_lookup
  on consumer_users(username_lookup);
create index if not exists consumer_users_created_at
  on consumer_users(created_at);

-- Rate limits are keyed by a server-secret HMAC of the username. No IP address
-- or browser fingerprint is retained.
create table if not exists consumer_auth_limits (
  limit_key text primary key,
  window_started_at integer not null,
  attempts integer not null
);

create index if not exists consumer_auth_limits_window
  on consumer_auth_limits(window_started_at);
