tp-twitter-hadoop
=================

#TELECOM PT twitter data analysis project.

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
Tweets on each line.
