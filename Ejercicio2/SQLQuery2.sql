CREATE DATABASE Netflix_1FN;
GO

USE Netflix_1FN;
GO


-- Tabla principal

CREATE TABLE shows (
    show_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(20),
    title VARCHAR(255),
    date_added DATE,
    release_year INT,
    rating VARCHAR(10),
    duration VARCHAR(50),
    description TEXT
);

-- tablas maestras

CREATE TABLE directors (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

CREATE TABLE actors (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(255) UNIQUE
);

CREATE TABLE countries (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(150) UNIQUE
);

CREATE TABLE genres (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(150) UNIQUE
);


-- Tablas relacionales (N:M)


CREATE TABLE show_directors (
    show_id VARCHAR(10),
    director_id INT,
    PRIMARY KEY (show_id, director_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id),
    FOREIGN KEY (director_id) REFERENCES directors(id)
);

CREATE TABLE show_cast (
    show_id VARCHAR(10),
    actor_id INT,
    PRIMARY KEY (show_id, actor_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id),
    FOREIGN KEY (actor_id) REFERENCES actors(id)
);

CREATE TABLE show_countries (
    show_id VARCHAR(10),
    country_id INT,
    PRIMARY KEY (show_id, country_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id),
    FOREIGN KEY (country_id) REFERENCES countries(id)
);

CREATE TABLE show_genres (
    show_id VARCHAR(10),
    genre_id INT,
    PRIMARY KEY (show_id, genre_id),
    FOREIGN KEY (show_id) REFERENCES shows(show_id),
    FOREIGN KEY (genre_id) REFERENCES genres(id)
);