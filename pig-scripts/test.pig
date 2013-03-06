/*
 * This script requires following parameters to be defined:
 *  - UDF_JAR_FILE
 *  - DATASET_FILE
 *  - TUPLES_WITH_ITEMS_ONLY_FILE
 */

--bin/pig -x local
--run -param_file test.params test.pig

REGISTER $UDF_JAR_FILE;

DEFINE BagConcat datafu.pig.bags.BagConcat();

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

/*
 * For user similarity based on items only.
 */
non_merged_tuples_with_items_only = FOREACH tweets_with_extracted_entities GENERATE $0 AS user_id:chararray, BagConcat($1, $2, com.oboturov.ht.pig.InvalidUrlRemover($3)) AS items:bag {T: tuple(mention:chararray)};
--dump non_merged_tuples_with_items_only;
--(@webkarnage,{(@Societysarah),(http://www.realmacsoftware.com/forums/index.php/forums/)})

non_merged_tuples_with_items_only_having_some_items = FILTER non_merged_tuples_with_items_only BY NOT IsEmpty(items);
--dump non_merged_tuples_with_items_only_having_some_items;

grouped_non_merged_tuples_with_items_only_having_some_items = GROUP non_merged_tuples_with_items_only_having_some_items BY user_id;
--dump grouped_non_merged_tuples_with_items_only_having_some_items;

merged_non_merged_tuples_with_items_only_having_some_items = FOREACH grouped_non_merged_tuples_with_items_only_having_some_items GENERATE group, com.oboturov.ht.pig.MergeGroupedBags($1);

STORE merged_non_merged_tuples_with_items_only_having_some_items INTO '$TUPLES_WITH_ITEMS_ONLY_FILE' USING PigStorage;

tuples_with_items_only_l = LOAD '$TUPLES_WITH_ITEMS_ONLY_FILE' AS (user_id_l:chararray, items_l:bag {T: tuple(item:chararray)});
tuples_with_items_only_r = LOAD '$TUPLES_WITH_ITEMS_ONLY_FILE' AS (user_id_r:chararray, items_r:bag {T: tuple(item:chararray)});

user_user_pairs_with_items_only = CROSS tuples_with_items_only_l, tuples_with_items_only_r;

user_user_pairs_with_items_only_similarity = FOREACH user_user_pairs_with_items_only GENERATE user_id_l, user_id_r, 1.0 - ((double)SIZE(DIFF(items_l, items_r)))/((double)SIZE(BagConcat(items_l, items_r))) AS sim:double;

result_similarity_with_items_only = FILTER user_user_pairs_with_items_only_similarity BY user_id_l != user_id_r;
