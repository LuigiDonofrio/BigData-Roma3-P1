ADD JAR SplitAndPurge.jar;
CREATE TEMPORARY FUNCTION split_and_purge AS 'it.uniroma3.Hive.UDF.SplitAndPurge';

SELECT word_year, word, occurencies
FROM (
	SELECT word2occurencies.*, RANK() OVER (PARTITION BY word_year ORDER BY occurencies DESC) AS rank
	FROM (
		SELECT word_year, word, COUNT(1) AS occurencies
		FROM (
			SELECT year(from_unixtime(CAST(time AS INT))) AS word_year, word
			FROM AmazonFineGoodsReviews LATERAL VIEW explode(split_and_purge(summary)) AmazonFineGoodsReviews AS word
		) AS year2word
		WHERE word_year IS NOT NULL
		GROUP BY word_year, word
	) AS word2occurencies
) AS top10
WHERE rank <= 10
