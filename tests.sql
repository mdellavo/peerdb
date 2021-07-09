CREATE TABLE foo(
  id INTEGER PRIMARY KEY,
  name STRING
);

CREATE UNIQUE INDEX idx_foo ON foo(name);

INSERT INTO foo VALUES (1, "foo"), (2, "bar"), (3, "baz"), (4, "qux");

SELECT * FROM foo;

DELETE FROM foo;

DROP INDEX idx_foo;

DROP TABLE foo;
