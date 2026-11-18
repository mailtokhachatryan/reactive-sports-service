CREATE TABLE sport (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    CONSTRAINT sport_name_trimmed_and_nonblank CHECK (name <> '' AND name = btrim(name))
);

CREATE UNIQUE INDEX sport_name_ci_unique ON sport (lower(name));
