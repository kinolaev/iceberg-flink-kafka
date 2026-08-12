create table sort_order(
  object_type text not null,
  object_id integer not null,
  name text not null,
  value text not null,
  primary key (object_id, name, object_type)
);

insert into sort_order(object_type, object_id, name, value) values
  ('User', 1, 'first_name', 'John'),
  ('User', 1, 'last_name', 'Doe');
