/*
 * This script requires following parameters to be defined:
 *  - UDF_JAR_FILE
 *  - DATASET_FILE
 */

--bin/pig -x local
--run -param_file test.params test.pig

REGISTER $UDF_JAR_FILE;

tweets = LOAD '$DATASET_FILE' USING PigStorage('\t') AS (T, time, user_id:chararray, text:chararray);
--dump tweets;
--(T,2009-06-11 16:56:43,http://twitter.com/gabanact,@SamanthaFoxx I mean I can agree Sunday)

normalized_tweets = FOREACH tweets GENERATE com.oboturov.ht.pig.SanitizeUserId(user_id) AS user_id:chararray, com.oboturov.ht.pig.SanitizeTweetText(text) AS text:chararray;
--dump normalized_tweets;
--(@gabanact,@SamanthaFoxx I mean I can agree Sunday)

sanitized_tweets = FILTER normalized_tweets BY user_id IS NOT NULL AND text IS NOT NULL;

tweets_with_extracted_entities = FOREACH sanitized_tweets GENERATE $0, FLATTEN(com.oboturov.ht.pig.TweetEntityExtractor($1));
--dump tweets_with_extracted_entities;
--(@gabanact,{(@SamanthaFoxx)},{(#MOA09)},{(http://bit.ly/1Lg4p)}, I mean I can agree Sunday)

non_merged_tuples = FOREACH tweets_with_extracted_entities GENERATE $0 AS user_id:chararray, $1 AS mentions:bag {T: tuple(mention:chararray)}, $2 AS hashtags:bag {T: tuple(hashtag:chararray)}, com.oboturov.ht.pig.InvalidUrlRemover($3) AS urls:bag {T: tuple(url:chararray)}, com.oboturov.ht.pig.TextTokenizer($4) AS tokens:bag {T: tuple(token:chararray)};
dump non_merged_tuples;
