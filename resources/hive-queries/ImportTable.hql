create external table if not exists AmazonFineGoodsReviews
( 
   	id BIGINT, 
	productid STRING,
	userid STRING, 
	profilename STRING, 
	helpfulnessnumerator INT, 
	helpfulnessdenominator INT, 
	score INT, 
	time BIGINT, 
	summary STRING, 
	text STRING
)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
TBLPROPERTIES('skip.header.line.count'='1');

LOAD DATA INPATH '/user/hive/input/Reviews.csv' INTO TABLE AmazonFineGoodsReviews;
