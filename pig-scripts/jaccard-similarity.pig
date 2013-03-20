REGISTER s3://tp-twitter-data-analysis/processing-scripts/twitter-jobs-standalone-aws-emr-jar-with-dependencies.jar;

DEFINE BagConcat datafu.pig.bags.BagConcat();

tuples_l = LOAD '$INPUT' AS (user_id_l:chararray, values_l:bag {T: tuple(item:chararray)});
tuples_r = LOAD '$INPUT' AS (user_id_r:chararray, values_r:bag {T: tuple(item:chararray)});

user_user_pairs = CROSS tuples_l, tuples_r;

user_user_pairs_similarity = FOREACH user_user_pairs GENERATE user_id_l, user_id_r, 1.0 - ((double)SIZE(DIFF(values_l, values_r)))/((double)SIZE(BagConcat(values_l, values_r))) AS sim:double;

result_similarity = FILTER user_user_pairs_similarity BY user_id_l != user_id_r AND sim > 0.0;

STORE result_similarity INTO '$OUTPUT' USING PigStorage;
