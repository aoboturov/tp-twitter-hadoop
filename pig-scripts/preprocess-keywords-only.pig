set mapred.compress.map.output true
set mapred.map.output.compression.code org.apache.hadoop.io.compress.GzipCodec
set pig.tmpfilecompression true
set pig.tmpfilecompression.codec org.apache.hadoop.io.compress.GzipCodec

REGISTER s3://tp-twitter-data-analysis/processing-scripts/twitter-jobs-standalone-personal-server-jar-with-dependencies.jar;

DEFINE BagConcat datafu.pig.bags.BagConcat();

tweets_with_extracted_entities = LOAD '$INPUT' USING PigStorage AS (user_id:chararray, mentions:bag {T: tuple(mention:chararray)}, hashtags:bag {T: tuple(hashtag:chararray)}, urls:bag {T: tuple(url:chararray)}, text:chararray);

non_merged_tuples_with_keywords_only = FOREACH tweets_with_extracted_entities GENERATE $0 AS user_id:chararray, com.oboturov.ht.pig.TextTokenizer($4) AS tokens:bag {T: tuple(token:chararray)};
--dump non_merged_tuples_with_keywords_only;
--(@webkarnage,{(aa),(bb),(cc)})

non_merged_tuples_having_only_some_items = FILTER non_merged_tuples_with_keywords_only BY NOT IsEmpty(tokens);
--dump non_merged_tuples_having_only_some_items;

grouped_non_merged_tuples_having_only_some_keywords = GROUP non_merged_tuples_having_only_some_items BY user_id;
--dump grouped_non_merged_tuples_having_only_some_keywords;

merged_non_merged_tuples_having_only_some_keywords = FOREACH grouped_non_merged_tuples_having_only_some_keywords GENERATE group, com.oboturov.ht.pig.MergeGroupedBags($1);

STORE merged_non_merged_tuples_having_only_some_keywords INTO '$OUTPUT' USING PigStorage;
