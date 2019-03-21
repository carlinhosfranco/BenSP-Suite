/* AUTORIGHTS
Copyright (C) 2007 Princeton University
	  
This file is part of Ferret Toolkit.

Ferret Toolkit is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2, or (at your option)
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software Foundation,
Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>
#include <cass.h>
#include <cass_timer.h>
#include <../image/image.h>
#include "tpool.h"
#include "queue.h"

//BenSP
#include <upl.h>

#define DEFAULT_DEPTH	25
#define MAXR	100
#define IMAGE_DIM	14

char *db_dir = NULL;
char *table_name = NULL;
char *query_dir = NULL;
char output_path[256];

char *file_log = NULL;

char out_latency_file[256];
char out_throughput_file[256];

double get_utime(){
	struct timeval current_time;	
	gettimeofday(&current_time,NULL);
	double time_now;
	time_now = (double)current_time.tv_sec * (double)1e6 + (double)current_time.tv_usec;
	return time_now;
}

double get_throughput_time(){
	struct timeval current_time;	
	gettimeofday(&current_time,NULL);
	double time_now;
	time_now = (double)current_time.tv_sec + (double)current_time.tv_usec/(double)1e6;

	return time_now;
}

FILE *fout;

//BenSP
int how_many_files = 0;
int total_files_to_alloc = 0;

typedef struct _metrics_t{

	int index_count;
	char *image_name;
	float size_file;
	double first_time;
	double last_time;
	double lat_load_seg;
	double lat_seg_extract;
	double lat_extract_vec;
	double lat_vec_rank;
	double lat_rank_out;

	double time_lapse;
  	double time_spent;
  	int current_dequeue;
 
  	int total_dequeue;
  	int last_value_dequeued;
  	
  	double throughput_by_image;
  	
  	int number_of_collect;
  	double mem_usage;
  	int nthreads;

  	double load_seg;
	double seg_ext;
	double ext_vec;
	double vec_rank;
	double rank_out;
	double service_time_mean;

}metrics_t;

double init_time;
#ifdef ENABLE_TRACING

FILE *latency_file;
FILE *throughput_file;

metrics_t *throughput_metric = NULL;
metrics_t *latency = NULL;
metrics_t *bensp_ferret_metrics;

char *alias_image_name;
double alias_image_size;
double alias_load_seg;
double alias_seg_ext;
double alias_ext_vec;
double alias_vec_rank;
double alias_rank_out;
double alias_first_time;
double alias_last_time;
int alias_idex_count;
int number_of_images;

double mean_load_seg = 0;
double mean_seg_ext = 0;
double mean_ext_vec = 0;
double mean_vec_rank = 0;
double mean_rank_out = 0;
double service_time = 0;
		
int last_value_dequeued = 0;
int collect_count = 0;
double time_now = 0.000;
double threshold_time = 1;
double baseline_time;


void print_metrics(char *bin, char *db, char *table_name, char *query, int topK, int threads, int load, int seg, int ext, int vec, int rank, double execution_time){

	char *path_log = getenv("FERRET_DIR_LOG");
	char filename[256];
	char file_throughput[256];

	sprintf(filename,"%s/all_latency_of_images_%d.dat",path_log,threads);
	
	latency_file = fopen(filename,"w");
	assert(latency_file != NULL);

	mean_load_seg = 0;
	mean_seg_ext = 0;
	mean_ext_vec = 0;
	mean_vec_rank = 0;
	mean_rank_out = 0;
	service_time = 0;

	fprintf(latency_file, "L->S \t S->E \t E->V \t V->R \t R->O \t Service Time \t FIRST TIME \t LAST TIME \t NAME \t SIZE FILE \n");
	
	for (int i = 0; i < how_many_files; i++){
		alias_image_name = (bensp_ferret_metrics+i)->image_name;
		alias_image_size = (bensp_ferret_metrics+i)->size_file;
		alias_load_seg = (bensp_ferret_metrics+i)->lat_load_seg;
		alias_seg_ext = (bensp_ferret_metrics+i)->lat_seg_extract;
		alias_ext_vec = (bensp_ferret_metrics+i)->lat_extract_vec;
		alias_vec_rank = (bensp_ferret_metrics+i)->lat_vec_rank;
		alias_rank_out = (bensp_ferret_metrics+i)->lat_rank_out;
		alias_first_time = (bensp_ferret_metrics+i)->first_time; 
		alias_last_time = (bensp_ferret_metrics+i)->last_time;
		
		fprintf(latency_file, "%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%s\t%.3lf \n", alias_load_seg,alias_seg_ext,alias_ext_vec,alias_vec_rank,alias_rank_out,alias_last_time - alias_first_time, alias_first_time,alias_last_time,alias_image_name,alias_image_size);

		mean_load_seg += alias_load_seg;
		mean_seg_ext += alias_seg_ext;
		mean_ext_vec += alias_ext_vec;
		mean_vec_rank += alias_vec_rank;
		mean_rank_out += alias_rank_out;

		service_time += alias_last_time - alias_first_time;

	}
	fprintf(latency_file, "#Command Line: %s %s %s %s(input) %d(images to rank) %d(threads) %d(L->S) %d(S->E) %d(E->V) %d(V->R) %d(R->O)\n",bin,db,table_name,query_dir,topK,threads,load,seg,ext,vec,rank);
	fclose(latency_file);

	sprintf(filename,"%s/real_time_latency_%d.dat",path_log,threads);
	sprintf(file_throughput,"%s/real_time_throughput_%d.dat",path_log,threads);

	latency_file = fopen(filename,"w");
	assert(latency_file != NULL);

	throughput_file = fopen(file_throughput,"w");
	assert(throughput_file != NULL);
	
	fprintf(throughput_file,"#Index \t Time Leapse(s) \t Time Spent(s) \t Current Dequeue \t Image Processed \t Throughput By Image \t Memmory Usage(kB)\n");
	fprintf(latency_file, "#IDX \t Time Lapse \t LOAD->SEG \t SEG->EXTRACT \t EXTRACT->VEC \t VEC->RANK \t RANK->OUT \t Service Time \n");

	for (int i = 0; i < collect_count; i++){
		fprintf(throughput_file,"%d\t%.3lf\t%.3lf\t%d\t%d\t%.3lf\t%.3lf\n",i,throughput_metric[i].time_lapse,throughput_metric[i].time_spent,throughput_metric[i].current_dequeue,throughput_metric[i].total_dequeue,throughput_metric[i].throughput_by_image,throughput_metric[i].mem_usage);
		fprintf(latency_file, "%d\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",i,latency[i].time_lapse, latency[i].load_seg, latency[i].seg_ext,latency[i].ext_vec,latency[i].vec_rank,latency[i].rank_out,latency[i].service_time_mean);
	}
	fprintf(latency_file, "#Command Line: %s %s %s %s(input) %d(images to rank) %d(threads) %d(L->S) %d(S->E) %d(E->V) %d(V->R) %d(R->O)\n",bin,db,table_name,query,topK,threads,load,seg,ext,vec,rank);
	fprintf(throughput_file,"#Command Line: %s %s %s %s(input) %d(images to rank) %d(threads) %d(L->S) %d(S->E) %d(E->V) %d(V->R) %d(R->O)\n",bin,db,table_name,query,topK,threads,load,seg,ext,vec,rank);
	
	fclose(latency_file);
	fclose(throughput_file);
	
	sprintf(filename,"%s/latency.dat",path_log);
	sprintf(file_throughput,"%s/throughput.dat",path_log);

	latency_file = fopen(filename,"a+");
	assert(latency_file != NULL);

	throughput_file = fopen(file_throughput,"a+");
	assert(throughput_file != NULL);
	
	fprintf(latency_file,"%d\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",threads,mean_load_seg/how_many_files, mean_seg_ext/how_many_files, mean_ext_vec/how_many_files, mean_vec_rank/how_many_files, mean_rank_out/how_many_files, service_time/how_many_files);
	fprintf(throughput_file,"%d\t%.3lf\n",threads,how_many_files/execution_time);

	if (UPL_getNumOfCores() == threads){
		fprintf(latency_file, "#threads \t LOAD->SEG \t SEG->EXTRACT \t EXTRACT->VEC \t VEC->RANK \t RANK->OUT \t Service Time \n");
		fprintf(throughput_file, "#threads \t mean_throughput\n");
	}

	fclose(latency_file);
	fclose(throughput_file);	

	free(bensp_ferret_metrics);
	free(throughput_metric);
	free(latency);

}

#endif

int NTHREAD_LOAD = 1;
int NTHREAD_SEG	= 1;
int NTHREAD_EXTRACT = 1;
int NTHREAD_VEC	= 1;
int NTHREAD_RANK = 1;
int NTHREAD_OUT	= 1;
//int DEPTH = DEFAULT_DEPTH;

//BenSP
int LOAD_STAGE = DEFAULT_DEPTH;
int SEG_STAGE = DEFAULT_DEPTH;
int EXTRACT_STAGE = DEFAULT_DEPTH;
int VEC_STAGE = DEFAULT_DEPTH;
int RANK_STAGE = DEFAULT_DEPTH;

int top_K = 10;

char *extra_params = "-L 8 - T 20";
//T = 50  L = 11  M = 14 

cass_env_t *env;
cass_table_t *table;
cass_table_t *query_table;

int vec_dist_id = 0;
int vecset_dist_id = 0;

struct load_data{
	int width, height;
	char *name;
	unsigned char *HSV, *RGB;
	float size_file;
	//BenSP
	int idx; 
	
};

struct queue q_load_seg;

struct seg_data{
	int width, height, nrgn;
	char *name;
	unsigned char *mask;
	unsigned char *HSV;
	//BenSP
	int idx;
};

struct queue q_seg_extract;

struct extract_data{
	cass_dataset_t ds;
	char *name;
	//BenSP
	int idx;
};

struct queue q_extract_vec;

struct vec_query_data{
	char *name;
	cass_dataset_t *ds;
	cass_result_t result;
	//BenSP
	int idx;
};

struct queue q_vec_rank;

struct rank_data{
	char *name;
	cass_dataset_t *ds;
	cass_result_t result;
	//BenSP
	int idx;
};

struct queue q_rank_out;


/* ------- The Helper Functions ------- */
int cnt_enqueue;
int cnt_dequeue;
char path[BUFSIZ];


int scan_dir (const char *, char *head);

int dir_helper (char *dir, char *head){
	DIR *pd = NULL;
	struct dirent *ent = NULL;
	int result = 0;
	pd = opendir(dir);
	
	if (pd == NULL) goto except;
	for (;;){

		ent = readdir(pd);
		if (ent == NULL) break;
		if (!strcmp(ent->d_name,".")){
			continue;
		} 
		if (!strcmp(ent->d_name,"..")){
			continue;
		}
		if ( ((strchr(ent->d_name,'.')) - ent->d_name+1) == 1 ){
			continue;
		}
		if (scan_dir(ent->d_name, head) != 0) return -1;
		
		++how_many_files;
	}
	goto final;

except:
	result = -1;
	perror("Error:");
final:
	if (pd != NULL) closedir(pd);
	return result;
}

/* the whole path to the file */
//int file_helper (const char *file){
int file_helper (const char *file, double sz_file, char *image_name){
	
	int r;
	struct load_data *data;
	
	data = (struct load_data *)malloc(sizeof(struct load_data));
	assert(data != NULL);

	data->name = strdup(file);
	r = image_read_rgb_hsv(file, &data->width, &data->height, &data->RGB, &data->HSV);
	assert(r == 0);
	
	/*
		r = image_read_rgb(file, &data->width, &data->height, &data->RGB);
		r = image_read_hsv(file, &data->width, &data->height, &data->HSV);
		*/
#ifdef ENABLE_TRACING
	data->idx = cnt_enqueue;
	data->size_file = sz_file;

	(bensp_ferret_metrics+cnt_enqueue)->index_count = cnt_enqueue;
	(bensp_ferret_metrics+cnt_enqueue)->image_name = strdup(image_name);
	(bensp_ferret_metrics+cnt_enqueue)->size_file = sz_file;
	(bensp_ferret_metrics+cnt_enqueue)->first_time = get_utime();
#endif //ENABLE_TRACING
	
	
	cnt_enqueue++;
	enqueue(&q_load_seg, data);
	return 0;
}

int scan_dir (const char *dir, char *head){
	
	struct stat st;
	int ret;
	
	/* test for . and .. */
	if (dir[0] == '.'){
		if (dir[1] == 0) return 0;
		else if (dir[1] == '.'){
			if (dir[2] == 0) return 0;
		}
	}

	
	strcat(head, dir);
	ret = stat(path, &st);


	if (ret != 0){
		perror("Error:");
		return -1;
	}
	if (S_ISREG(st.st_mode)){
		//BenSP Comment: head = imagem name
		file_helper(path,st.st_size,head);
	}
	else if (S_ISDIR(st.st_mode)){
		strcat(head, "/");
		dir_helper(path, head + strlen(head));
	}
	/* removed the appended part */
	head[0] = 0;
	return 0;
}


/* ------ The Stages ------ */
void *t_load (void *dummy){

	const char *dir = (const char *)dummy;

	path[0] = 0;

#ifdef ENABLE_TRACING
	char temp_buff[256];
	sprintf(temp_buff,"ls %s | wc -l", dir);
	
	char *number_files = UPL_getCommandResult(temp_buff);

	total_files_to_alloc = atoi(number_files);
	number_of_images = total_files_to_alloc;

	bensp_ferret_metrics = (metrics_t*)malloc(total_files_to_alloc*sizeof(metrics_t));
	assert(bensp_ferret_metrics != NULL);
#endif

	if (strcmp(dir, ".") == 0){
		dir_helper(".", path);
	}
	else{
		scan_dir(dir, path);
	}

	queue_signal_terminate(&q_load_seg);
	
	return NULL;
}

void *t_seg (void *dummy){

	struct seg_data *seg;
	struct load_data *load;

	double diff_time = 0;
	int idx_stage;
	
	while(1){

		if(dequeue(&q_load_seg, &load) < 0)
			break;
				
		assert(load != NULL);
		seg = (struct seg_data *)calloc(1, sizeof(struct seg_data));

#ifdef ENABLE_TRACING
		diff_time = get_utime();		
		idx_stage = load->idx;		
		
		(bensp_ferret_metrics+idx_stage)->lat_load_seg = diff_time-(bensp_ferret_metrics+idx_stage)->first_time;
#endif
	
		seg->name = load->name;

		seg->width = load->width;
		seg->height = load->height;
		seg->HSV = load->HSV;
		
		image_segment(&seg->mask, &seg->nrgn, load->RGB, load->width, load->height);
		free(load->RGB);
		free(load);

#ifdef ENABLE_TRACING

		(bensp_ferret_metrics+idx_stage)->lat_seg_extract = get_utime();
		seg->idx = idx_stage;
#endif
		enqueue(&q_seg_extract, seg);		
	}
	
	queue_signal_terminate(&q_seg_extract);
	return NULL;

}

void *t_extract (void *dummy){

	struct seg_data *seg;
	struct extract_data *extract;
	
	double diff_time = 0;
	int idx_stage;
	
	while (1){
	
		if(dequeue(&q_seg_extract, &seg) < 0)
			break;
		
		assert(seg != NULL);
		extract = (struct extract_data *)calloc(1, sizeof(struct extract_data));

#ifdef ENABLE_TRACING		
		diff_time = get_utime();
		extract->name = seg->name;
		idx_stage = seg->idx;
		
		(bensp_ferret_metrics+idx_stage)->lat_seg_extract = diff_time - (bensp_ferret_metrics+idx_stage)->lat_seg_extract;		
#endif
		image_extract_helper(seg->HSV, seg->mask, seg->width, seg->height, seg->nrgn, &extract->ds);

		free(seg->mask);
		free(seg->HSV);
		free(seg);

#ifdef ENABLE_TRACING
		extract->idx = idx_stage;
		(bensp_ferret_metrics+idx_stage)->lat_extract_vec = get_utime();
#endif
		enqueue(&q_extract_vec, extract);
	}
		
	queue_signal_terminate(&q_extract_vec);
	return NULL;
}

void *t_vec (void *dummy){
	
	struct extract_data *extract;
	struct vec_query_data *vec;
	cass_query_t query;
		
	//BenSP
	double diff_time = 0;
	int idx_stage;
	
	while(1){

		if(dequeue(&q_extract_vec, &extract) < 0)
			break;
		
		assert(extract != NULL);
		vec = (struct vec_query_data *)calloc(1, sizeof(struct vec_query_data));
		
#ifdef ENABLE_TRACING		
		diff_time = get_utime();
		idx_stage = extract->idx;
		(bensp_ferret_metrics+idx_stage)->lat_extract_vec = diff_time - (bensp_ferret_metrics+idx_stage)->lat_extract_vec;
#endif

		vec->name = extract->name;		

		memset(&query, 0, sizeof query);
		query.flags = CASS_RESULT_LISTS | CASS_RESULT_USERMEM;

		vec->ds = query.dataset = &extract->ds;
		query.vecset_id = 0;

		query.vec_dist_id = vec_dist_id;

		query.vecset_dist_id = vecset_dist_id;

		query.topk = 2*top_K;

		query.extra_params = extra_params;
		
		cass_result_alloc_list(&vec->result, vec->ds->vecset[0].num_regions, query.topk);
			
		cass_table_query(table, &query, &vec->result);

#ifdef ENABLE_TRACING		
		vec->idx = idx_stage;
		(bensp_ferret_metrics+idx_stage)->lat_vec_rank = get_utime();
#endif

		enqueue(&q_vec_rank, vec);
	}
	
	queue_signal_terminate(&q_vec_rank);
	return NULL;
}

void *t_rank (void *dummy){
	
	struct vec_query_data *vec;
	struct rank_data *rank;
	cass_result_t *candidate;
	cass_query_t query;
	
	//BenSP
	double diff_time = 0;
	int idx_stage;
	
		
	while (1){

		if(dequeue(&q_vec_rank, &vec) < 0)
			break;
		
		assert(vec != NULL);

		rank = (struct rank_data *)calloc(1, sizeof(struct rank_data));

#ifdef ENABLE_TRACING		
		diff_time = get_utime();
		idx_stage = vec->idx;		
		(bensp_ferret_metrics+idx_stage)->lat_vec_rank = diff_time - (bensp_ferret_metrics+idx_stage)->lat_vec_rank;
#endif
		rank->name = vec->name;

		query.flags = CASS_RESULT_LIST | CASS_RESULT_USERMEM | CASS_RESULT_SORT;
		query.dataset = vec->ds;
		query.vecset_id = 0;

		query.vec_dist_id = vec_dist_id;

		query.vecset_dist_id = vecset_dist_id;

		query.topk = top_K;

		query.extra_params = NULL;

		candidate = cass_result_merge_lists(&vec->result, (cass_dataset_t *)query_table->__private, 0);
		query.candidate = candidate;

		cass_result_alloc_list(&rank->result, 0, top_K);
		
		cass_table_query(query_table, &query, &rank->result);

		cass_result_free(&vec->result);
		cass_result_free(candidate);
		free(candidate);
		cass_dataset_release(vec->ds);
		free(vec->ds);
		free(vec);

#ifdef ENABLE_TRACING
		rank->idx = idx_stage;
		(bensp_ferret_metrics+idx_stage)->lat_rank_out = get_utime();
#endif
		enqueue(&q_rank_out, rank);
	}
		
	queue_signal_terminate(&q_rank_out);
	return NULL;
}

void *t_out (void *dummy){
	
	struct rank_data *rank;
	
	//BenSP
	double diff_time = 0;
	int idx_stage;
	
#ifdef ENABLE_TRACING	
	
	throughput_metric = (metrics_t*)malloc(1500*sizeof(metrics_t));
	latency = (metrics_t*)malloc(1500*sizeof(metrics_t));
	
	baseline_time = get_throughput_time();
	
#endif 	
	
	while (1){
		
		if(dequeue(&q_rank_out, &rank) < 0)
			break;
		
		assert(rank != NULL);

#ifdef ENABLE_TRACING
		
		diff_time = get_utime();
		idx_stage = rank->idx;

		(bensp_ferret_metrics+idx_stage)->last_time = diff_time;
		(bensp_ferret_metrics+idx_stage)->lat_rank_out = diff_time - (bensp_ferret_metrics+idx_stage)->lat_rank_out;

		mean_load_seg += (bensp_ferret_metrics+idx_stage)->lat_load_seg;
		mean_seg_ext += (bensp_ferret_metrics+idx_stage)->lat_seg_extract;
		mean_ext_vec += (bensp_ferret_metrics+idx_stage)->lat_extract_vec;
		mean_vec_rank += (bensp_ferret_metrics+idx_stage)->lat_extract_vec;
		mean_rank_out += (bensp_ferret_metrics+idx_stage)->lat_rank_out;
		service_time += (bensp_ferret_metrics+idx_stage)->last_time - (bensp_ferret_metrics+idx_stage)->first_time;

		time_now = get_throughput_time();
		if ( (time_now - threshold_time) >= baseline_time ){
			
			throughput_metric[collect_count].time_lapse = time_now - init_time;
			latency[collect_count].time_lapse = time_now - init_time;
			
			double time_spent = time_now - baseline_time;
			throughput_metric[collect_count].time_spent = time_spent;
			latency[collect_count].time_spent = time_spent;

			throughput_metric[collect_count].current_dequeue = cnt_dequeue;// - last_value_dequeued;
			throughput_metric[collect_count].throughput_by_image = (cnt_dequeue - last_value_dequeued) / time_spent;
			throughput_metric[collect_count].total_dequeue = cnt_dequeue - last_value_dequeued;
			
			latency[collect_count].load_seg = mean_load_seg / (cnt_dequeue - last_value_dequeued);
			latency[collect_count].seg_ext = mean_seg_ext / (cnt_dequeue - last_value_dequeued);
			latency[collect_count].ext_vec = mean_ext_vec / (cnt_dequeue - last_value_dequeued);
			latency[collect_count].vec_rank = mean_vec_rank / (cnt_dequeue - last_value_dequeued);
			latency[collect_count].rank_out = mean_rank_out / (cnt_dequeue - last_value_dequeued);
			latency[collect_count].service_time_mean = service_time / (cnt_dequeue - last_value_dequeued);

			last_value_dequeued = cnt_dequeue;

			mean_load_seg = 0;
			mean_seg_ext = 0;
			mean_ext_vec = 0;
			mean_vec_rank = 0;
			mean_rank_out = 0;
			service_time = 0;

			throughput_metric[collect_count].mem_usage = (double)UPL_getProcMemUsage();

			collect_count++;

			baseline_time = get_throughput_time();
		}
#endif
		
		ARRAY_BEGIN_FOREACH(rank->result.u.list, cass_list_entry_t p){
			char *obj = NULL;
			if (p.dist == HUGE) continue;
			cass_map_id_to_dataobj(query_table->map, p.id, &obj);
			assert(obj != NULL);
			fprintf(fout,"%s:%g\t", obj, p.dist);
		} ARRAY_END_FOREACH;
#ifdef ENABLE_TRACING
		fprintf(fout,"\t\t%s\n",(bensp_ferret_metrics+idx_stage)->image_name);
#else
		fprintf(fout,"\t\t%s\n",rank->name);
#endif		
		
		cass_result_free(&rank->result);
		
		free(rank->name);
		free(rank);

		cnt_dequeue++;
		
	}

#ifdef ENABLE_TRACING
	time_now = get_throughput_time();
			
	throughput_metric[collect_count].time_lapse = time_now - init_time;
	latency[collect_count].time_lapse = time_now - init_time;
			
	double time_spent = time_now - baseline_time;
	throughput_metric[collect_count].time_spent = time_spent;
	latency[collect_count].time_spent = time_spent;

	throughput_metric[collect_count].current_dequeue = cnt_dequeue;
	throughput_metric[collect_count].throughput_by_image = (cnt_dequeue - last_value_dequeued) / time_spent;
	throughput_metric[collect_count].total_dequeue = cnt_dequeue - last_value_dequeued;
			
	latency[collect_count].load_seg = mean_load_seg / (cnt_dequeue - last_value_dequeued);
	latency[collect_count].seg_ext = mean_seg_ext / (cnt_dequeue - last_value_dequeued);
	latency[collect_count].ext_vec = mean_ext_vec / (cnt_dequeue - last_value_dequeued);
	latency[collect_count].vec_rank = mean_vec_rank / (cnt_dequeue - last_value_dequeued);
	latency[collect_count].rank_out = mean_rank_out / (cnt_dequeue - last_value_dequeued);
	latency[collect_count].service_time_mean = service_time / (cnt_dequeue - last_value_dequeued);

	throughput_metric[collect_count].mem_usage = (double)UPL_getProcMemUsage();

	collect_count++;

	baseline_time = get_throughput_time();
#endif	
	
	assert(cnt_enqueue == cnt_dequeue);
	return NULL;
}

int main (int argc, char *argv[]){

	stimer_t tmr;
	stimer_t time_exec;

	tdesc_t *t_load_desc;
	tdesc_t *t_seg_desc;
	tdesc_t *t_extract_desc;
	tdesc_t *t_vec_desc;
	tdesc_t *t_rank_desc;
	tdesc_t *t_out_desc;

	tpool_t *p_load;
	tpool_t *p_seg;
	tpool_t *p_extract;
	tpool_t *p_vec;
	tpool_t *p_rank;
	tpool_t *p_out;

	double execution_time, query_time;

	int ret, i;
	
	if (argc < 11){

		printf("%s <database> <table> <query dir> <top K> <threads> <que_LOAD> <que_SEG> <que_EXT> <que_VEC> <que_RANK> \n", argv[0]);
		printf("./ferret corel lsh queries 5 1 5 5 5 5 5'\n");
		return 0;
	}

	db_dir = argv[1]; 
	table_name = argv[2]; 
	query_dir = argv[3];
	top_K = atoi(argv[4]); 

	NTHREAD_SEG = atoi(argv[5]);
	NTHREAD_EXTRACT = atoi(argv[5]);
	NTHREAD_VEC = atoi(argv[5]);
	NTHREAD_RANK = atoi(argv[5]);

	LOAD_STAGE = atoi(argv[6]); 
	SEG_STAGE = atoi(argv[7]);
	EXTRACT_STAGE = atoi(argv[8]);
	VEC_STAGE = atoi(argv[9]);
	RANK_STAGE = atoi(argv[10]);
	
	char *path_log = getenv("FERRET_DIR_LOG");
		
	sprintf(output_path,"%s/output.txt",path_log);
	fout = fopen(output_path, "w");
	assert(fout != NULL);
	
	stimer_tick(&time_exec);
	init_time = get_throughput_time();

	cass_init();

	ret = cass_env_open(&env, db_dir, 0);
	if (ret != 0) { printf("ERROR: %s\n", cass_strerror(ret)); return 0; }

	vec_dist_id = cass_reg_lookup(&env->vec_dist, "L2_float");
	
	assert(vec_dist_id >= 0);

	vecset_dist_id = cass_reg_lookup(&env->vecset_dist, "emd");
	assert(vecset_dist_id >= 0);

	i = cass_reg_lookup(&env->table, table_name);


	table = query_table = cass_reg_get(&env->table, i);

	i = table->parent_id;

	if (i >= 0){
		query_table = cass_reg_get(&env->table, i);
	}

	if (query_table != table) cass_table_load(query_table);
	
	cass_map_load(query_table->map);

	cass_table_load(table);

	image_init(argv[0]);

	stimer_tick(&tmr);
	
	queue_init(&q_load_seg,    LOAD_STAGE, NTHREAD_LOAD);
	queue_init(&q_seg_extract, SEG_STAGE, NTHREAD_SEG);
	queue_init(&q_extract_vec, EXTRACT_STAGE, NTHREAD_EXTRACT);
	queue_init(&q_vec_rank,    VEC_STAGE, NTHREAD_VEC);
	queue_init(&q_rank_out,    RANK_STAGE, NTHREAD_RANK);

	t_load_desc = (tdesc_t *)calloc(NTHREAD_LOAD, sizeof(tdesc_t));
	t_seg_desc = (tdesc_t *)calloc(NTHREAD_SEG, sizeof(tdesc_t));
	t_extract_desc = (tdesc_t *)calloc(NTHREAD_EXTRACT, sizeof(tdesc_t));
	t_vec_desc = (tdesc_t *)calloc(NTHREAD_VEC, sizeof(tdesc_t));
	t_rank_desc = (tdesc_t *)calloc(NTHREAD_RANK, sizeof(tdesc_t));
	t_out_desc = (tdesc_t *)calloc(NTHREAD_OUT, sizeof(tdesc_t));

	t_load_desc[0].attr = NULL;
	t_load_desc[0].start_routine = t_load;
	t_load_desc[0].arg = query_dir;

	for (i = 1; i < NTHREAD_LOAD; i++) t_load_desc[i] = t_load_desc[0];

	t_seg_desc[0].attr = NULL;
	t_seg_desc[0].start_routine = t_seg;
	t_seg_desc[0].arg = NULL;

	for (i = 1; i < NTHREAD_SEG; i++) t_seg_desc[i] = t_seg_desc[0];

	t_extract_desc[0].attr = NULL;
	t_extract_desc[0].start_routine = t_extract;
	t_extract_desc[0].arg = NULL;

	for (i = 1; i < NTHREAD_EXTRACT; i++) t_extract_desc[i] = t_extract_desc[0];

	t_vec_desc[0].attr = NULL;
	t_vec_desc[0].start_routine = t_vec;
	t_vec_desc[0].arg = NULL;
	for (i = 1; i < NTHREAD_VEC; i++) t_vec_desc[i] = t_vec_desc[0];

	t_rank_desc[0].attr = NULL;
	t_rank_desc[0].start_routine = t_rank;
	t_rank_desc[0].arg = NULL;
	for (i = 1; i < NTHREAD_RANK; i++) t_rank_desc[i] = t_rank_desc[0];


	t_out_desc[0].attr = NULL;
	t_out_desc[0].start_routine = t_out;
	t_out_desc[0].arg = NULL;
	for (i = 1; i < NTHREAD_OUT; i++) t_out_desc[i] = t_out_desc[0];

	cnt_enqueue = cnt_dequeue = 0;

	p_load = tpool_create(t_load_desc, NTHREAD_LOAD);
	p_seg = tpool_create(t_seg_desc, NTHREAD_SEG);
	p_extract = tpool_create(t_extract_desc, NTHREAD_EXTRACT);
	p_vec = tpool_create(t_vec_desc, NTHREAD_VEC);
	p_rank = tpool_create(t_rank_desc, NTHREAD_RANK);
	p_out = tpool_create(t_out_desc, NTHREAD_OUT);

	tpool_join(p_out, NULL);
	tpool_join(p_rank, NULL);
	tpool_join(p_vec, NULL);
	tpool_join(p_extract, NULL);
	tpool_join(p_seg, NULL);
	tpool_join(p_load, NULL);

	tpool_destroy(p_load);
	tpool_destroy(p_seg);
	tpool_destroy(p_extract);
	tpool_destroy(p_vec);
	tpool_destroy(p_rank);
	tpool_destroy(p_out);

	free(t_load_desc);
	free(t_seg_desc);
	free(t_extract_desc);
	free(t_vec_desc);
	free(t_rank_desc);
	free(t_out_desc);

	queue_destroy(&q_load_seg);
	queue_destroy(&q_seg_extract);
	queue_destroy(&q_extract_vec);
	queue_destroy(&q_vec_rank);
	queue_destroy(&q_rank_out);

	query_time = stimer_tuck(&tmr, "QUERY TIME");

	ret = cass_env_close(env, 0);

	if (ret != 0) { printf("ERROR: %s\n", cass_strerror(ret)); return 0; }

	cass_cleanup();

	image_cleanup();

	
	execution_time = stimer_tuck(&time_exec, "\nFerret Time Processing");
	FILE *performance_time;
	char out_performance_time[256];// = "../temp/execution_time.dat";
	sprintf(out_performance_time,"%s/execution_time.dat",path_log);
	performance_time = fopen(out_performance_time,"a+");
	
	fprintf(performance_time,"%d\t%.2lf\t%.2lf\t%.2lf #threads/query_time/execution_time/mem_usage(kB)\n",NTHREAD_SEG,query_time,execution_time,(double)UPL_getProcMemUsage());
	if (UPL_getNumOfCores() == NTHREAD_SEG){
	
		fprintf(performance_time, "#Command Line: %s %s %s %s(input) %d(images to rank) %d(threads) %d(L->S) %d(S->E) %d(E->V) %d(V->R) %d(R->O)\n",argv[0],db_dir,table_name,query_dir,top_K,NTHREAD_SEG,LOAD_STAGE,SEG_STAGE,EXTRACT_STAGE,VEC_STAGE,RANK_STAGE);
	}

	fclose(performance_time);
	fclose(fout);

	sprintf(out_performance_time,"%s/time_%d_%dth.dat",path_log,LOAD_STAGE,NTHREAD_SEG);
	performance_time = fopen(out_performance_time,"a+");
	fprintf(performance_time,"%d\t%.2lf\t%.2lf\t%.2lf #threads/query_time/execution_time/mem_usage(kB)\n",NTHREAD_SEG,query_time,execution_time,(double)UPL_getProcMemUsage());
	
	fclose(performance_time);

#ifdef ENABLE_TRACING
	
	print_metrics(argv[0],db_dir,table_name,query_dir,top_K,NTHREAD_SEG,LOAD_STAGE,SEG_STAGE,EXTRACT_STAGE,VEC_STAGE,RANK_STAGE,execution_time);
	
#endif
	
	return 0;
}

