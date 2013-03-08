records = LOAD 'data' AS (word:chararray, year:int, count:int, pages:int, books:int);
grouped_records = GROUP records BY word;
counts = FOREACH grouped_records GENERATE group, SUM(records.count);
sorted_counts = ORDER counts BY $1 DESC;
topk = LIMIT sorted_counts 10;
DUMP topk;