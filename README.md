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

Now input files could be handled within Hadoop framework without having too much problem.

