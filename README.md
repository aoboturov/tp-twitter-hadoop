tp-twitter-hadoop
=================

#TELECOM PT twitter data analysis project.

## Description

Starting from the Twitter dataset:

*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-06.txt.gz
*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-07.txt.gz
*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-08.txt.gz
*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-09.txt.gz
*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-10.txt.gz
*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-11.txt.gz
*    http://snap.stanford.edu/data/bigdata/twitter7/tweets2009-12.txt.gz

One can generate similarity network (with Jaccard similarity measure) between users whose tweets present there.

## Project compilation

This is a Hadoop Java client. It supports two sets of maven profiles:

*    v0.20.205.0 (the `mariane` profile);
*    v2.0.1-alpha (the `personal-server` profile);
*    v1.0.3 (the `aws-emr` profile);
*    with Pig embedded (the `standalone` profile).

To build project one should call Maven 3:
`mvn -P ${my_profile_name} clean package assembly:single`

# Second iteration (to be used with AWS-EMR)

Project was realised with Pig, Hadoop, some Python and Java.

To process data these actions should be executed:

* First data must be treated with `/pig-scripts/tweets_file_normalizer.py` to produce a tuple per row data instead of four-lined format of entries.
* Then the `/pig-scripts/tweets-preprocessing.pig` should be launched to produce Pig relations of form: `(user_id:chararray, mentions:bag {T: tuple(mention:chararray)}, hashtags:bag {T: tuple(hashtag:chararray)}, urls:bag {T: tuple(url:chararray)}, text:chararray)`. Those relations are basis for similarity network construction.
* `/pig-scripts/preprocess-items-only.pig` and `/pig-scripts/preprocess-keywords-only.pig` construct tuples with items only and with keywords only. Joint similarity (by items and keywords) could be dudeced in a similar fashion.
* To generate a similarity network from ouput of a previous step one should execute the following Java program: `java -Xmx1024m -cp twitter-jobs-standalone-standalone-jar-with-dependencies.jar com.oboturov.ht.crossjoin.InMemoryCrossJoin part-r-00000 jaccard-sim.zip`. Action is time and space consuming.

# Fist iteration (not to be used in production)

## Improtant considerations for data processing with Hadoop

Pay attention to heap memory consimption: jobs should take special care when using *heavy* resources (e.g. Date parsers, Language identifiers, etc)

Consider not use reducers - this will make processing faster.

## Raw data preparation

One *MUST* check that initial archives are not corrupted.

```bash
bunzip2 -t FILE_NAME.bz2
```

Actual file data format with tweets differs from the one specified in README.txt, so normalization *MUST* be performed in advance.
File has additional line at the beginning, contating the total number of tweets in that file - this line *MUST* be removed.

```bash
bunzip2 FILE_NAME.bz2
tail -n +2 FILE_NAME > FILE_NAME_NORMALIZED
bzip2 FILE_NAME_NORMALIZED
```

Check that newly produced archive is not corrupted.

```bash
bunzip2 -t FILE_NAME_NORMALIZED
```

```bash
cat FILE_NAME | grep "No Post Title" | wc -l
```

Gives a count of tweets which had no text message and hence *MUST* be discarded from further treatment.

Now input files could be handled within Hadoop framework without having too much problem.

## Stage-0 Raw Data processing

One have to execute the `com.oboturov.ht.stage0.TweetsGenerator` Hadoop Script which will generate a file with
Tweets on each line. It has 4 custom counters:

1.    `com.oboturov.ht.stage0.TweetsReader$Map$Counters#ILLEGAL_DATE` - showing # of tweets discarded because of illegal date;
2.    `com.oboturov.ht.stage0.TweetsReader$Map$Counters#NON_NORMALIZABLE_USER_NAME` - # of tweets with unrecognized user name format;
3.    `com.oboturov.ht.stage0.TweetsReader$Map$Counters#EMPTY_POSTS` - # of tweets with no post message;
4.    `com.oboturov.ht.stage0.TweetsReader$Map$Counters#TWEETS_READ` - total # of tweets read from an input file.

## Stage-1 Generated Tweets Data processing

The `com.oboturov.ht.stage1.TweetsCounter` Script will output a single number which is the number of Tweets contained in
provided file.

Anoter Script called `com.oboturov.ht.stage1.UsersListGenerator` will generate K-V pair for User name and an amount of
Tweets he produced during the period.

Key script of this stage is `com.oboturov.ht.stage1.NupletsGenerator`. It produces a nuplet for Hash-tag, Mention or URL in tweet.

1.    `com.oboturov.ht.stage1.NupletCreator$Map$Counters#SKIPPED_CASHTAG_NUPLET` - # of recognized but discarded Cash-tags;
2.    `com.oboturov.ht.stage1.NupletCreator$Map$Counters#ILLEGAL_TWEET_ENTITY_TYPE` - illegal entity type, expcted to be zero;
3.    `com.oboturov.ht.stage1.NupletCreator$Map$Counters#NUPLETS_WITH_ITEMS_GENERATED` - total # of nuplets with Items generated for tweets from input file.
4.    `com.oboturov.ht.stage1.NupletCreator$Map$Counters#NUPLETS_WITH_ONLY_KEYWORDS_GENERATED` - total # of nuplets having only Keywords generated for tweets from input file.

## Stage-2 Keywords processing

The single script on this stage is the `com.oboturov.ht.stage2.KeywordsProcessing`.

1.    `com.oboturov.ht.stage2.LanguageIdentificationWithLangGuess$LanguageIdentificationMap$Counters#NUPLETS_WITH_ITEMS_BUT_LANGUAGE_NOT_IDENTIFIED` - # of nuplets where language of the Keyword text was impossible to recognize;
2.    `com.oboturov.ht.stage2.LanguageIdentificationWithLangGuess$LanguageIdentificationMap$Counters#NUPLETS_DISCARDED_BECAUSE_LANGUAGE_WAS_NOT_IDENTIFIED_AND_NO_ITEMS` - # of nuplets with no Items where language of the Keyword text was impossible to recognize, hence they were discarded from future processing;
3.    `com.oboturov.ht.stage2.PhraseTokenizer$PhraseTokenizerMap$Counters#PRODUCED_NUPLETS_WITH_ITEMS_ONLY` - # of nuplets for which identified language was not supported by stemmer but they had an Item and were passed further as Nuplets with no Keyword;
4.    `com.oboturov.ht.stage2.PhraseTokenizer$PhraseTokenizerMap$Counters#PRODUCED_NUPLETS_WITH_STEMMED_KEYWORDS` - # of nuplets generated on the stemming phase;
5.    `com.oboturov.ht.stage2.PhraseTokenizer$PhraseTokenizerMap$Counters#NUPLETS_WITH_NO_KEYWORD` - # of nuplets passed though without a keyword.
6.    `com.oboturov.ht.stage2.PhraseTokenizer$PhraseTokenizerMap$Counters#LANGUAGE_NOT_SUPPORTED_AND_NO_ITEMS` - # of nuplets discarded because it was impossible to identify their language and they had no Item.

Another script `com.oboturov.ht.stage2.NupletsWithKeywordsProcessedSplitter` is useful to split Nuplets produced after Keywords were processed into different groups for further processsing.
It creates 4 files called:

1.    `nuplets-with-no-items.txt`
2.    `nuplets-requiring-uris-resolution.txt`
3.    `nuplets-with-no-keywords-and-no-uris.txt`
4.    `nuplets-with-keywords-and-no-uris.txt`
