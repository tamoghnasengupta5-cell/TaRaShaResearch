-- TaRaSha Company Lens: free-tier founding-user schema.
-- Run this in a dedicated Supabase project. Never point it at the research database.

create extension if not exists pgcrypto;

create table public.beta_invites (
  email text primary key check (email = lower(email)),
  invited_at timestamptz not null default now(),
  accepted_at timestamptz,
  invited_by uuid references auth.users(id)
);

create table public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text not null,
  display_name text,
  role text not null default 'member' check (role in ('member', 'admin')),
  created_at timestamptz not null default now()
);

create table public.companies (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  name text not null,
  symbol text not null,
  sector text not null,
  description text not null,
  founded smallint,
  employee_display text,
  is_published boolean not null default false,
  updated_at timestamptz not null default now()
);

create table public.data_sources (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null references public.companies(id) on delete cascade,
  title text not null,
  source_url text not null,
  published_on date,
  retrieved_at timestamptz not null default now(),
  notes text
);

create table public.financial_facts (
  id bigint generated always as identity primary key,
  company_id uuid not null references public.companies(id) on delete cascade,
  metric_key text not null check (metric_key in ('revenue', 'operating_margin', 'free_cash_flow', 'net_debt')),
  fiscal_year smallint not null,
  period_end date not null,
  value numeric(24, 6) not null,
  unit text not null,
  currency char(3),
  source_id uuid not null references public.data_sources(id),
  is_restated boolean not null default false,
  created_at timestamptz not null default now(),
  unique (company_id, metric_key, period_end, source_id)
);

create table public.watchlists (
  user_id uuid not null references auth.users(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (user_id, company_id)
);

create table public.consent_records (
  id bigint generated always as identity primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  document_key text not null,
  document_version text not null,
  accepted_at timestamptz not null default now(),
  unique (user_id, document_key, document_version)
);

create index financial_facts_company_metric_period_idx
  on public.financial_facts (company_id, metric_key, period_end desc);

alter table public.beta_invites enable row level security;
alter table public.profiles enable row level security;
alter table public.companies enable row level security;
alter table public.data_sources enable row level security;
alter table public.financial_facts enable row level security;
alter table public.watchlists enable row level security;
alter table public.consent_records enable row level security;

create function public.is_admin() returns boolean
language sql stable security definer set search_path = '' as $$
  select exists (
    select 1 from public.profiles
    where id = auth.uid() and role = 'admin'
  );
$$;

create function public.handle_invited_user() returns trigger
language plpgsql security definer set search_path = '' as $$
begin
  perform pg_advisory_xact_lock(819247);
  if not exists (select 1 from public.beta_invites where email = lower(new.email)) then
    raise exception 'This preview is invitation-only.';
  end if;
  if (select count(*) from public.profiles) >= 10 then
    raise exception 'The ten-user founding preview is full.';
  end if;
  insert into public.profiles (id, email, display_name)
  values (new.id, lower(new.email), coalesce(new.raw_user_meta_data ->> 'display_name', ''));
  update public.beta_invites set accepted_at = now() where email = lower(new.email);
  return new;
end;
$$;

create trigger on_auth_user_created
  after insert on auth.users
  for each row execute procedure public.handle_invited_user();

create function public.update_my_display_name(new_display_name text) returns void
language plpgsql security definer set search_path = '' as $$
begin
  if auth.uid() is null then raise exception 'Authentication required.'; end if;
  update public.profiles
  set display_name = left(trim(new_display_name), 80)
  where id = auth.uid();
end;
$$;

create policy "members read published companies" on public.companies
  for select to authenticated using (is_published or public.is_admin());
create policy "members read published company sources" on public.data_sources
  for select to authenticated using (
    exists (select 1 from public.companies c where c.id = company_id and (c.is_published or public.is_admin()))
  );
create policy "members read published financial facts" on public.financial_facts
  for select to authenticated using (
    exists (select 1 from public.companies c where c.id = company_id and (c.is_published or public.is_admin()))
  );
create policy "users read own profile" on public.profiles
  for select to authenticated using (id = auth.uid() or public.is_admin());
create policy "users manage own watchlist" on public.watchlists
  for all to authenticated using (user_id = auth.uid()) with check (user_id = auth.uid());
create policy "users read own consents" on public.consent_records
  for select to authenticated using (user_id = auth.uid());
create policy "users add own consents" on public.consent_records
  for insert to authenticated with check (user_id = auth.uid());

-- Admin writes use the Supabase service-role key only from a protected ingestion job.
-- The service-role key must never be placed in VITE_* variables or browser code.
