SELECT p1, p2, count(uid) as users 
FROM (
	SELECT t1.productid as p1, t2.productid as p2, t1.userid as uid
      	FROM AmazonFineGoodsReviews t1 
      	JOIN AmazonFineGoodsReviews t2 ON t1.userid = t2.userid 
      	WHERE t1.productid < t2.productid AND t1.productid != t2.productid 
      ) AS biproduct2user
GROUP BY p1, p2
ORDER BY p1;


