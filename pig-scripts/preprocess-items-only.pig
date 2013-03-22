REGISTER s3://tp-twitter-data-analysis/processing-scripts/twitter-jobs-standalone-aws-emr-jar-with-dependencies.jar;

DEFINE BagConcat datafu.pig.bags.BagConcat();

tweets_with_extracted_entities = LOAD '$INPUT' USING PigStorage AS (user_id:chararray, mentions:bag {T: tuple(mention:chararray)}, hashtags:bag {T: tuple(hashtag:chararray)}, urls:bag {T: tuple(url:chararray)}, text:chararray);

non_merged_tuples_with_items_only = FOREACH tweets_with_extracted_entities GENERATE $0 AS user_id:chararray, BagConcat($1, $2, com.oboturov.ht.pig.InvalidUrlRemover($3)) AS items:bag {T: tuple(mention:chararray)};
--dump non_merged_tuples_with_items_only;
--(@webkarnage,{(@Societysarah),(http://www.realmacsoftware.com/forums/index.php/forums/)})

non_merged_tuples_having_only_some_items = FILTER non_merged_tuples_with_items_only BY NOT IsEmpty(items);
--dump non_merged_tuples_having_only_some_items;

grouped_non_merged_tuples_having_only_some_items = GROUP non_merged_tuples_having_only_some_items BY user_id;
--dump grouped_non_merged_tuples_having_only_some_items;

merged_non_merged_tuples_having_only_some_items = FOREACH grouped_non_merged_tuples_having_only_some_items GENERATE group, com.oboturov.ht.pig.MergeGroupedBags($1);

STORE merged_non_merged_tuples_having_only_some_items INTO '$OUTPUT' USING PigStorage;
