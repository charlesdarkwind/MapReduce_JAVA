CREATE TABLE crypto (
    symbol          STRING,
    interval        STRING,
    time            BIGINT,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '/home/cloudera/crypto.csv' INTO TABLE crypto


SELECT * FROM crypto;


select
    substr(symbol, 2, length(symbol) - 2) as symbol,
    round(((stddev_samp(close) * 100) / avg(close)), 2) as Ecart_type
from (
    select symbol, close,
    row_number() over (partition by symbol) rn
    from crypto
    where `interval` = "\"1h\""
) A
where rn <= 100
group by symbol
order by Ecart_type;
