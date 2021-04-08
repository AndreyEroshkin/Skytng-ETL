CREATE IF NOT EXIST SCHEMA etl;



CREATE TABLE etl.order (
  id bigint primary key,
  student_id bigint,
  teacher_id bigint,
  stage varchar(10),
  status varchar(512),
  created_at timestamp,
  updated_at timestamp
);

INSERT INTO etl.order (
id,
student_id,
teacher_id,
stage,
status,
created_at,
updated_at
)
VALUES
(1, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(2, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(3, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(4, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(5, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(6, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(7, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00'),
(8, 111, 222, 'first', 'active', '2020-01-01 10:10:10+03:00', '2020-01-01 10:10:10+03:00');


