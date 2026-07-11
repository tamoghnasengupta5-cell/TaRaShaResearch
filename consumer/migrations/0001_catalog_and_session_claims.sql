create table if not exists company_catalog (
  id text primary key,
  cik text,
  name text not null,
  ticker text not null,
  exchange text not null,
  country text not null check (country in ('USA', 'India')),
  provider text not null,
  research_available integer not null default 0 check (research_available in (0, 1)),
  updated_at integer not null
);

create unique index if not exists company_catalog_country_ticker_exchange
  on company_catalog(country, ticker, exchange);
create index if not exists company_catalog_search_name on company_catalog(country, name);
create index if not exists company_catalog_search_ticker on company_catalog(country, ticker);

create table if not exists session_claims (
  session_id text not null,
  company_id text not null references company_catalog(id) on delete cascade,
  from_year integer not null,
  to_year integer not null,
  created_at integer not null,
  primary key(session_id, company_id)
);

create index if not exists session_claims_expiry on session_claims(created_at);
