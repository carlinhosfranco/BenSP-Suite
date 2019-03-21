#!/bin/bash
#bensp_api -p dedup -i $1 -t 1 -a chunk 32 fr 20 "dd" 20 comp 20 rr 20 notrace
CORES=$3
bensp_api -p dedup -i $1 -t 1 -a chunk 32 fr $2 "dd" $2 comp $2 rr $2 trace
rm ../../apps/dedup/temp/* -rf
for (( i = 2; i <= $CORES; i++ )); do
      if [[ $i == 1 || $i == 2 ]]; then
              bensp_api -p dedup -i $1 -t $i
              i=`expr $i + 1`
      fi
        sleep 2
        i=`expr $i + 1`
        bensp_api -p dedup -i $1 -t $i
        rm ../../apps/dedup/temp/* -rf
done

rm ../../apps/dedup/temp/* -rf

mv ../../logs/dedup/run* .

