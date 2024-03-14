DROP DATABASE If EXISTS museum;

DROP TABLE IF EXISTS exhibitions CASCADE;
DROP TABLE IF EXISTS floors CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS rating_event;
DROP TABLE IF EXISTS support_event;
DROP TABLE IF EXISTS rating_value;
DROP TABLE IF EXISTS support_value;

CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE floors (
    floor_id INT PRIMARY KEY,
    floor_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE exhibitions (
    exhibition_id INT PRIMARY KEY,
    exhibition_code VARCHAR(20),
    exhibition_name VARCHAR(255) UNIQUE NOT NULL,
    exhibition_start_date DATE NOT NULL,
    exhibition_description TEXT NOT NULL,
    department_id INT NOT NULL,
    floor_id INT NOT NULL,
    FOREIGN KEY (department_id)
            REFERENCES departments(department_id)
            ON DELETE CASCADE,
    FOREIGN KEY (floor_id)
            REFERENCES floors(floor_id)
            ON DELETE CASCADE,
);
