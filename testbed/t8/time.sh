#!/bin/bash
CORES=$3
#bensp_api -p dedup -i $1 -t 1 -a chunk 2048 fr 10 "dd" 10 comp 10 rr 10 notrace
bensp_api -p dedup -i $1 -t 1 -a chunk 2048 fr $2 "dd" $2 comp $2 rr $2 notrace
rm ../../apps/dedup/temp/* -rf
for (( j = 1; j <= 10; j++ )); do
	for (( i = 1; i <= $CORES; i++ )); do
        if [[ $i == 1 || $i == 2 ]]; then
                bensp_api -p dedup -i $1 -t $i
                rm ../../apps/dedup/temp/* -rf
                sleep 2
        else       
        i=`expr $i + 1`
        bensp_api -p dedup -i $1 -t $i
        rm ../../apps/dedup/temp/* -rf
        fi
	done
rm ../../apps/dedup/temp/* -rf
done

rm ../../apps/dedup/temp/* -rf

mv ../../logs/dedup/run* .

