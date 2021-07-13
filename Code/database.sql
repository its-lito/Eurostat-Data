CREATE DATABASE countries;
USE countries;

CREATE TABLE IF NOT EXISTS tot_nights(
	country SET('EL', 'PT') NOT NULL,
	year YEAR,
	value INT
);

CREATE TABLE IF NOT EXISTS foreign_nights(
	country SET('EL', 'PT') NOT NULL,
	year YEAR,
	value INT
);

CREATE TABLE IF NOT EXISTS tot_arrives(
	country SET('EL', 'PT') NOT NULL,
	year YEAR,
	value INT
);

CREATE TABLE IF NOT EXISTS foreign_arrives(
	country SET('EL', 'PT') NOT NULL,
	year YEAR,
	value INT
);
