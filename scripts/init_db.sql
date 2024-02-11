BEGIN;
CREATE TABLE IF NOT EXISTS source_file
(
    date_begin DATE,
    date_end DATE,
    date_scanned DATE,
    path TEXT, 
    account INT
);
CREATE TABLE IF NOT EXISTS account
(
    name TEXT,
    bank TEXT,
    folder TEXT,
    deposit FLOAT
);
CREATE TABLE IF NOT EXISTS operation
(
    date DATE,
    payee TEXT,
    motif TEXT,
    label TEXT,
    amount FLOAT NOT NULL,
    currency TEXT,
    category TEXT,
    source INT NOT NULL
);
CREATE TABLE IF NOT EXISTS category
(
    name TEXT
);
CREATE TABLE IF NOT EXISTS linked_operations
(
    link INT,
    op1 INT,
    op2 INT
);
CREATE TABLE IF NOT EXISTS link
(
    name TEXT
);
COMMIT;