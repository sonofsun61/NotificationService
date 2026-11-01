create table processed_events (
    event_id uuid primary key,
    processed_at timestamptz not null default now()
);