set mapred.compress.map.output true
set mapred.map.output.compression.code org.apache.hadoop.io.compress.GzipCodec
set pig.tmpfilecompression true
set pig.tmpfilecompression.codec org.apache.hadoop.io.compress.GzipCodec

REGISTER s3://tp-twitter-data-analysis/processing-scripts/twitter-jobs-standalone-personal-server-jar-with-dependencies.jar;

tweets = LOAD '$INPUT' USING PigStorage('\t') AS (time, user_id:chararray, text:chararray);
--dump tweets;
--(2009-06-11 16:56:43,http://twitter.com/gabanact,@SamanthaFoxx I mean I can agree Sunday)

normalized_tweets = FOREACH tweets GENERATE com.oboturov.ht.pig.SanitizeUserId(user_id) AS user_id:chararray, com.oboturov.ht.pig.SanitizeTweetText(text) AS text:chararray;
--dump normalized_tweets;
--(@gabanact,@SamanthaFoxx I mean I can agree Sunday)

sanitized_tweets = FILTER normalized_tweets BY user_id IS NOT NULL AND text IS NOT NULL;

tweets_with_extracted_entities = FOREACH sanitized_tweets GENERATE $0, FLATTEN(com.oboturov.ht.pig.TweetEntityExtractor($1));
--dump tweets_with_extracted_entities;
--(@gabanact,{(@SamanthaFoxx)},{(#MOA09)},{(http://bit.ly/1Lg4p)}, I mean I can agree Sunday)

STORE tweets_with_extracted_entities INTO '$OUTPUT' USING PigStorage;
