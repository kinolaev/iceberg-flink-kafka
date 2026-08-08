create table logicaltypes(
  id uuid primary key,
  dec numeric(8, 4),
  varscaledec numeric,
  date date,
  time time(3),
  microtime time(6),
  ts timestamp(3),
  microts timestamp(6),
  tstz timestamp(3) with time zone,
  microtstz timestamp(6) with time zone
);

insert into logicaltypes(id, dec, varscaledec, date, time, microtime, ts, microts, tstz, microtstz)
  values(uuidv7(), 1.2, 3.4, '2026-08-06', '16:30:00.123', '16:30:00.123456',
    '2026-08-06 16:30:00.123'   , '2026-08-06 16:30:00.123456',
    '2026-08-06 16:30:00.123+02', '2026-08-06 16:30:00.123456+02');
