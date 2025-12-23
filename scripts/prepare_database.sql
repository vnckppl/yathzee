CREATE TABLE vincent (
    name TEXT PRIMARY KEY NOT NULL,
    gamedate DATETIME NOT NULL,
    roll1 INTEGER,
    roll2 INTEGER,
    roll3 INTEGER,
    roll4 INTEGER,
    roll5 INTEGER,
    roll6 INTEGER,
    fullh INTEGER,
    same3 INTEGER,
    same4 INTEGER,
    strsm INTEGER,
    strlg INTEGER,
    chanc INTEGER,
    yathz INTEGER
    );


-- Import the CSV into this table:
.mode csv
.import --skip 1 /datadisk/Private/Python/20251222_Yathzee/database/vincent.csv yathzee






-- Convert empty values to NULL
UPDATE nomnom SET review = NULL WHERE TRIM(review) = '';
UPDATE nomnom SET health = NULL WHERE TRIM(health) = '';


-- Information about table
-- .schema movies


-- Display names for the first four movies
SELECT name FROM nomnom LIMIT 4;
