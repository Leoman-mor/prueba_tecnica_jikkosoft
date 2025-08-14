-- Limpieza para rehacer el esquema durante las pruebas
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS hired_employees;
DROP TABLE IF EXISTS jobs;

-- Tablas de dimensiones
CREATE TABLE departments (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE jobs (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

-- Tabla de hecho
CREATE TABLE hired_employees (
  id             INTEGER PRIMARY KEY,
  name           TEXT NOT NULL,
  datetime       TIMESTAMP NOT NULL,
  department_id  INTEGER NOT NULL REFERENCES departments(id),
  job_id         INTEGER NOT NULL REFERENCES jobs(id)
);
