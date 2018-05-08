add jar SplitAndPurge.jar;
CREATE TEMPORARY FUNCTION split_and_purge AS 'it.uniroma3.hive.UserDefinedFunctions.SplitAndPurge';

SELECT word_year, word, occurencies
FROM (
	SELECT word_year, word, COUNT(1) AS occurencies, ROW_NUMBER() OVER (PARTITION BY word_year) AS row_number
	FROM (
		SELECT year(from_unixtime(CAST(time AS INT))) AS word_year, word
		FROM amazonfinegoodsreviews LATERAL VIEW explode(split_and_purge(summary)) amazonfinegoodsreviews AS word
	) AS year2word
	WHERE word_year IS NOT NULL
	GROUP BY word_year, word
) AS word2occurencies
ORDER BY word DESC
