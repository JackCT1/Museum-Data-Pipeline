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

CREATE TABLE rating_events (
    id INT GENERATED ALWAYS AS IDENTITY,
    exhibit_id INT,
    rating_value_id INT,
    rated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (rating_id),
    FOREIGN KEY (exhibit_id)
            REFERENCES exhibitions(id)
            ON DELETE CASCADE,
    FOREIGN KEY (rating_value_id)
            REFERENCES rating_values(id)
            ON DELETE CASCADE
);

CREATE TABLE rating_values (
    id INT PRIMARY KEY,
    value SMALLINT UNIQUE NOT NULL,
    description TEXT UNIQUE NOT NULL
);

CREATE TABLE support_events (
    id INT GENERATED ALWAYS AS IDENTITY,
    exhibit_id INT,
    support_value_id INT,
    made_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (support_event_id),
    FOREIGN KEY (exhibit_id)
            REFERENCES exhibitions(id)
            ON DELETE CASCADE,
    FOREIGN KEY (support_value_id)
            REFERENCES support_values(id)
            ON DELETE CASCADE
);

CREATE TABLE support_values (
    id INT GENERATED ALWAYS AS IDENTITY,
    value SMALLINT NOT NULL,
    description VARCHAR(255) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (support_value, support_description)
);