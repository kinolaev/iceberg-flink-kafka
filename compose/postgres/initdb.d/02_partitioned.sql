create table partitioned(
  id uuid primary key default uuidv7(),
  "createdAt" timestamp(6) with time zone not null default now()
);

insert into partitioned(id, "createdAt") values
  (uuidv7(), '2026-08-03 16:30:00.123456+02'),
  (uuidv7(), '2026-08-04 16:30:00.123456+02'),
  (uuidv7(), '2026-08-05 16:30:00.123456+02'),
  (uuidv7(), '2026-08-06 16:30:00.123456+02');
