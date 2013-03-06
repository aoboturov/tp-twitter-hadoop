/*
 * This script requires following parameters to be defined:
 *  - UDF_JAR_FILE
 *  - DATASET_FILE
 */

--bin/pig -x local
--run -param_file test.params test.pig

REGISTER $UDF_JAR_FILE;

tweets = LOAD '$DATASET_FILE' USING PigStorage('\t') AS (T, time, user_id:chararray, text:chararray);

normalized_tweets = FOREACH tweets GENERATE com.oboturov.ht.pig.SanitizeUserId(user_id) AS user_id:chararray, com.oboturov.ht.pig.SanitizeTweetText(text) AS text:chararray;

sanitized_tweets = FILTER normalized_tweets BY user_id IS NOT NULL AND text IS NOT NULL;

dump sanitized_tweets;
