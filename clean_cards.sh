#!/bin/bash

ls */fields.csv | sed 's/.fields.csv//' | while read dir ; do
        cd $dir;
        ls dashboard_*json | while read dash ; do
                jq . < "$dash"  | grep card_nam | sed 's/.*%card_id%//' | sed 's/,*$//' | sort -u | awk -F '"' '{gsub(/['"'"\/]'/, "*", $1); print "find . -name '"'"'card_"$1".json'"'"' -delete "}' ;
        done  | bash
        cd -;
done
