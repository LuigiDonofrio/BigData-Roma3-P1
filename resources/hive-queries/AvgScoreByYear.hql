SELECT productid, score_year, AVG(float_score)
FROM (
	SELECT productid, year(from_unixtime(CAST(time AS INT))) AS score_year, CAST(score AS FLOAT) AS float_score
	FROM AmazonFineGoodsReviews
) AS product2score
WHERE score_year >= 2003 AND score_year <= 2012
GROUP BY productid, score_year
ORDER BY productid, score_year;
