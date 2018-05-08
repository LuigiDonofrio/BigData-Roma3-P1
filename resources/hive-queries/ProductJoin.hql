SELECT p2u1.productid, p2u2.productid, COUNT(1) as occurencies
FROM (
	SELECT productid, userid
	FROM AmazonFineGoodsReviews
) AS p2u1
JOIN (
	SELECT productid, userid
	FROM AmazonFineGoodsReviews
) AS p2u2
ON p2u1.userid = p2u2.userid
WHERE p2u1.productid != p2u2.productid AND p2u1.productid < p2u2.productid
GROUP BY p2u1.productid, p2u2.productid;
