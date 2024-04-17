DROP TABLE IF EXISTS exhibitions CASCADE;
DROP TABLE IF EXISTS floors CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS rating_events CASCADE;
DROP TABLE IF EXISTS support_events CASCADE;
DROP TABLE IF EXISTS rating_values CASCADE;
DROP TABLE IF EXISTS support_values CASCADE;

CREATE TABLE departments (
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(255) UNIQUE NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE floors (
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(50) UNIQUE NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE exhibitions (
    id INT GENERATED ALWAYS AS IDENTITY,
    code VARCHAR(20),
    name VARCHAR(255) UNIQUE NOT NULL,
    start_date DATE NOT NULL,
    description TEXT NOT NULL,
    department_id INT NOT NULL,
    floor_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (department_id)
            REFERENCES departments(id),
    FOREIGN KEY (floor_id)
            REFERENCES floors(id)
);

CREATE TABLE rating_values (
    id INT GENERATED ALWAYS AS IDENTITY,
    value SMALLINT UNIQUE NOT NULL,
    description TEXT UNIQUE NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE rating_events (
    id INT PRIMARY KEY,
    exhibit_id INT,
    rating_value_id INT,
    rated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (exhibit_id)
            REFERENCES exhibitions(id),
    FOREIGN KEY (rating_value_id)
            REFERENCES rating_values(id)
);

CREATE TABLE support_values (
    id INT GENERATED ALWAYS AS IDENTITY,
    value SMALLINT UNIQUE NOT NULL,
    description VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE support_events (
    id INT PRIMARY KEY,
    exhibit_id INT,
    support_value_id INT,
    made_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (exhibit_id)
            REFERENCES exhibitions(id),
    FOREIGN KEY (support_value_id)
            REFERENCES support_values(id)
);

INSERT INTO departments(name)
VALUES
('Entomology'),
('Geology'),
('Paleontology'),
('Zoology'),
('Ecology');

INSERT INTO floors(name)
VALUES
('Vault'),
('1'),
('2'),
('3');

INSERT INTO exhibitions (code, name, start_date, description, department_id, floor_id)
VALUES
('EXH_00', 'Measureless to Man','2021-08-23', 'An immersive 3D experience: delve deep into a previously-inaccessible cave system.', 2, 2),
('EXH_01', 'Adaption', '2019-07-01', 'How insect evolution has kept pace with an industrialised world', 1, 1),
('EXH_02', 'The Crenshaw Collection', '2021-03-03', 'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.', 4, 3),
('EXH_03', 'Cetacean Sensations', '2019-07-01', 'Whales: from ancient myth to critically endangered.', 4, 2),
('EXH_04', 'Our Polluted World', '2021-05-12', 'A hard-hitting exploration of humanity`s impact on the environment.', 5, 4),
('EXH_05', 'Thunder Lizards', '2023-02-01', 'How new research is making scientists rethink what dinosaurs really looked like.', 3, 1);

INSERT INTO rating_values (value, description)
VALUES
(0, 'Terrible'),
(1, 'Bad'),
(2, 'Neutral'),
(3, 'Good'),
(4, 'Amazing');

INSERT INTO support_values (value, description)
VALUES
(0, 'Assistance'),
(1, 'Emergency');