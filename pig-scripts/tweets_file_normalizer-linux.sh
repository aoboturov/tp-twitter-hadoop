#!/bin/sh

cat $1 | sed ':a;N;$!ba;s/\nU//g' | sed ':a;N;$!ba;s/\nW//g' | sed ':a;N;$!ba;s/\nT/T/g' > $1-prepared.txt
