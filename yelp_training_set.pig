/*
 yelp business rate system, data processing
 'https://www.kaggle.com/c/yelp-recsys-2013/data', ‘yelp_training_set.zip’
*/

REGISTER 'path_to_pig/piggybank.jar';
REGISTER 'path_to_pig/elephantbirdhadoopcompat4.14RC2.jar';
REGISTER 'path_to_pig/elephantbirdcore4.14RC2.jar';
REGISTER 'path_to_pig/elephantbirdpig4.14RC2.jar';
REGISTER 'path_to_pig/jsonsimple1.1.1.jar';
--register all jars before start. Or there might be errors.




-- Use ‘Business’ data to find get business with rating below 3.0
businessRaw = LOAD 'hdfs://m1.mt.dataapplab.com:8020/user/acdepand/pig/pig_hw/yelp_training_set/yelp_training_set_business.json'
    USING com.twitter.elephantbird.pig.load.JsonLoader();
DUMP businessRaw;
    
--business = FOREACH b GENERATE (Bag())$0#'neighborhoods' as bNeighbor;
business = FOREACH businessRaw GENERATE (chararray)$0#'type' as bType, (chararray)$0#'business_id' as bID,
                                        (chararray)$0#'name' as bName, $0#'neighborhoods' as bNeighbor,
                                        (chararray)$0#'full_address' as bAddr, (chararray)$0#'city' as bCity,
                                        (chararray)$0#'state' as bState, (float)$0#'latitude' as bLatitude,
                                        (float)$0#'longtitude' as bLongtitude, (float)$0#'stars' as bStars,
                                        (int)$0#'review_count' as revCount, $0#'categories' as bCats,
                                        (Boolean)$0#'open' as open;
--Multiple lines in 'full_address', so can do:
--(chararray)REPLACE($0#'full_address', '\\n', ',');

businessStarLessThan3 = FILTER business BY (bStars < 3.0);
businessStarLessThan3Count = FOREACH (GROUP businessStarLessThan3 ALL) GENERATE COUNT(businessStarLessThan3);
--1692
--Can also use SPLIT/IF/OTHERWISE





-- Use ‘Checkin’ data to find the business for those who have checkins of more than 10 on Wednesday betwwen 11AM-12PM
checkinRaw = LOAD 'hdfs://m1.mt.dataapplab.com:8020/user/acdepand/pig/pig_hw/yelp_training_set/yelp_training_set_checkin.json'
    USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

checkin = FOREACH checkinRaw GENERATE (chararray)$0#'type' as cType, (chararray)$0#'business_id' as cID,
                                      (map[int])$0#'checkin_info' as cInfo; 
                                      --map key type must be chararray

checkinFiltered = FILTER checkin BY cInfo#'11-2' >= 10;





--Use ‘Review’ data to get review info after 2012-06-01
reviewRaw = LOAD 'hdfs://m1.mt.dataapplab.com:8020/user/acdepand/pig/pig_hw/yelp_training_set/yelp_training_set_review.json'
    USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);    
review = FOREACH reviewRaw GENERATE (chararray)json#'type' as rType, (chararray)json#'business_id' as rID,
                                    (chararray)json#'userID' as rUser, (float)json#'stars' as rStar,
                                    (chararray)json#'text' as rText, (chararray)json#'date' as rDate,
                                    (map[int])json#'votes' as rVotes;
reviewAfter120601 = FILTER review BY (rDate > '2012-06-01');
reviewAfter120601Count = FOREACH (GROUP reviewAfter120601 ALL) GENERATE COUNT(reviewAfter120601); 
dump reviewAfter120601Count;





--Use ‘User’ data to get business with rating below 3.0
userRaw = LOAD 'hdfs://m1.mt.dataapplab.com:8020/user/acdepand/pig/pig_hw/yelp_training_set/yelp_training_set_user.json'
    USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');
user = FOREACH userRaw GENERATE (chararray)$0#'type' as uType, (chararray)$0#'userID' as uUser, 
                                (chararray)$0#'name' as uName, (int)$0#'review_count' as uRevCount,
                                (float)$0#'stars' as uStars, (map[])$0#'votes' as uVotes;
SPLIT user INTO user_group_1 IF uStars < 3.0, user_group_2 OTHERWISE;
STORE user_group_1 INTO 'path/to/new/folder' USING PigStorage('\u0001');