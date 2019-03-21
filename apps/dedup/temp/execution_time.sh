#!/bin/bash
CORES=$3
#bensp_api -p dedup -i $1 -t 1 -a chunk 32 fr 20 "dd" 20 comp 20 rr 20 notrace
bensp_api -p dedup -i $1 -t 1 -a chunk 32 fr $2 "dd" $2 comp $2 rr $2 notrace
rm /home/carlos/bensp/apps/dedup/temp/2018-05* -rf
for (( j = 1; j <= 10; j++ )); do
	for (( i = 1; i <= $CORES; i++ )); do
        if [[ $i == 1 || $i == 2 ]]; then
                bensp_api -p dedup -i $1 -t $i -a run
                rm /home/carlos/bensp/apps/dedup/temp/2018-05* -rf
                sleep 2
        else       
        i=`expr $i + 1`
        bensp_api -p dedup -i $1 -t $i -a run
        rm /home/carlos/bensp/apps/dedup/temp/2018-05* -rf
        fi
	done
rm /home/carlos/bensp/apps/dedup/temp/2018-05* -rf
done

rm /home/carlos/bensp/apps/dedup/temp/2018-05* -rf

mv ../../logs/dedup/run* .

