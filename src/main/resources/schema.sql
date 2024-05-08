CREATE TABLE IF NOT EXISTS AccountOperation (
    id SERIAL primary key,
    accountNumber varchar(255),
    "date" date,
    operation varchar(10),
    "value" decimal,
    channel varchar(10),
    success varchar(10)
);