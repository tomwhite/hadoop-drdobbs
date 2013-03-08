CREATE EXTERNAL TABLE words (word STRING, year INT, occurrences INT, pages INT, books INT)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
LOCATION '/user/cloudera/data';
