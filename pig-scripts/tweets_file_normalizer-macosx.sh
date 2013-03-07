#!/bin/sh

cat $1 | sed -e ':a' -e 'N' -e '$!ba' -e 's/\nU//g' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\nW//g' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\nT/T/g' > $1-prepared.txt
