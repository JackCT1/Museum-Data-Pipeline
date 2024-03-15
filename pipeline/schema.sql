DROP DATABASE If EXISTS museum;

DROP TABLE IF EXISTS exhibitions CASCADE;
DROP TABLE IF EXISTS floors CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS rating_events CASCADE;
DROP TABLE IF EXISTS support_events CASCADE;
DROP TABLE IF EXISTS rating_values CASCADE;
DROP TABLE IF EXISTS support_values CASCADE;

CREATE TABLE departments (
    id INT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE floors (
    id INT PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE exhibitions (
    id INT PRIMARY KEY,
    code VARCHAR(20),
    name VARCHAR(255) UNIQUE NOT NULL,
    start_date DATE NOT NULL,
    description TEXT NOT NULL,
    department_id INT NOT NULL,
    floor_id INT NOT NULL,
    FOREIGN KEY (department_id)
            REFERENCES departments(id)
            ON DELETE CASCADE,
    FOREIGN KEY (floor_id)
            REFERENCES floors(id)
            ON DELETE CASCADE
);

CREATE TABLE rating_events ();

CREATE TABLE rating_values (
    id INT PRIMARY KEY,
    value SMALLINT UNIQUE NOT NULL,
    description TEXT UNIQUE NOT NULL
);

CREATE TABLE support_events ();

CREATE TABLE support_values ();