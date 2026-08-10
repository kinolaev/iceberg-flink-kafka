create table partitioned(
  id uuid primary key,
  "createdAt" timestamp(6) with time zone not null
);

insert into partitioned(id, "createdAt") values
  (uuidv7(), '2026-08-03 16:30:00.123456+02'),
  (uuidv7(), '2026-08-04 16:30:00.123456+02'),
  (uuidv7(), '2026-08-05 16:30:00.123456+02'),
  (uuidv7(), '2026-08-06 16:30:00.123456+02');
