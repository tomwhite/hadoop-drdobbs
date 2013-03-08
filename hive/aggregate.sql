SELECT word, SUM(occurrences)
FROM words
GROUP BY word;