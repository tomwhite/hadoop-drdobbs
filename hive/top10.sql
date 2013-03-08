SELECT word, SUM(occurrences) AS occurrences
FROM words
GROUP BY word
ORDER BY occurrences DESC
LIMIT 10;