/* ****************************************************************************
* Copyright (c) 2006-2009 Princeton University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*    * Redistributions of source code must retain the above copyright
*      notice, this list of conditions and the following disclaimer.
*    * Redistributions in binary form must reproduce the above copyright
*      notice, this list of conditions and the following disclaimer in the
*      documentation and/or other materials provided with the distribution.
*    * Neither the name of Princeton University nor the
*      names of its contributors may be used to endorse or promote products
*      derived from this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY PRINCETON UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL PRINCETON UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
****************************************************************************
*/

#include <assert.h>
#include <strings.h>
#include <math.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <stdio.h>
#include <dirent.h>

#ifdef ENABLE_UPL
#include "upl.h" //UPL 
#endif

#include "util.h"
#include "dedupdef.h"
#include "encoder.h"
#include "debug.h"
#include "hashtable.h"
#include "config.h"
#include "rabin.h"
#include "mbuffer.h"

//#ifdef ENABLE_PTHREADS
#include "queue.h"
#include "binheap.h"
#include "tree.h"
//#endif //ENABLE_PTHREADS

#ifdef ENABLE_GZIP_COMPRESSION
#include <zlib.h>
#endif //ENABLE_GZIP_COMPRESSION

#ifdef ENABLE_BZIP2_COMPRESSION
#include <bzlib.h>
#endif //ENABLE_BZIP2_COMPRESSION

#ifdef ENABLE_PTHREADS
#include <pthread.h>
#endif //ENABLE_PTHREADS

#ifdef ENABLE_PARSEC_HOOKS
#include <hooks.h>
#endif //ENABLE_PARSEC_HOOKS

#define INITIAL_SEARCH_TREE_SIZE 4096

//BenSP Comment: ID global para contar a qunatidade de chunks. Esse valor é usado para identificar eles na aplicação, idependentemente dos arquivos.
long int global_id = 0;

double init_time, end_time, BSPtime;

struct timeval get_init_time;
struct timeval get_end_time;
chunk_t *BSP_chunk;

typedef struct {
	char *filename;
	uintmax_t size_file;
	void *file_loading;
}get_file_t;

int total_files = 0;
int how_many_files = 0;

void print_time(double time_processing, int nthreads){
	
	char *path_log = getenv("DEDUP_DIR_LOG");
	char filename[255];
	sprintf(filename,"%s/time_%d_%dth.dat",path_log,NWINDOW,nthreads);
	FILE *time_file;

	time_file = fopen(filename,"a+");
	fprintf(time_file,"%d\t%.2lf\n",nthreads,time_processing);
	fclose(time_file);

	sprintf(filename,"%s/execution_time_%d.dat",path_log,NWINDOW);
	time_file = fopen(filename,"a+");
	fprintf(time_file,"%d\t%.2lf\n",nthreads,time_processing);
	if (UPL_getRealNumCores() == nthreads){
		fprintf(time_file, "#threads \t Time Processing \n");
	}
	fclose(time_file);
}

#ifdef ENABLE_TRACING
/*
BenSP Comment: Struct de coleta para cálculo de throughput
*/

metrics_t *colect_metrics;

int nthreads = 0;
int number_of_collect = 0;

/*
BenSP Comment: Variáveis para cálculo da latência média e latência em tempo real. Função também imprime as métricas de throughput em tempo real e o total da aplicação.
Resumo: Cada chunk carrega seu clock. O tempo é coletado quando o chunk é inserido no buffer e quando retirado, a diferença é calculada. 
		Assim, cada chunk tem sua latência calculada quando chegar no estágio final (RR).
		A rotina salva todos os tempos de cada chunk em um arquivo.
		A latência é coletada em microsegundos. 
*/

latency *get_lat = NULL;
latency *mean_latency = NULL;
latency *realloc_get_latency = NULL;

int number_of_collect_latency = 0;
int current_chunk_dd = 0; 
int current_chunk_not_dd = 0;
void print_bensp_metrics(int tot_chunks, metrics_t *print_metrics, double execution_time, double size_total, double mean_chunk_size, double stddev_chunk){

	char *path_log = getenv("DEDUP_DIR_LOG");
	char filename[255];
	sprintf(filename,"%s/all_chunk_latency_%dth.dat",path_log,nthreads);
	FILE *lat_file;
	lat_file = fopen(filename,"w");
	
	float mean_fr_dd = 0;
	float mean_dd_comp = 0;
	float mean_comp_rr = 0;
	float mean_dd_rr = 0;
	float service_time = 0;

	fprintf(lat_file, "#ID \t IDCHUNK \t FR->Dedup \t Dedup->comp \t Dedup->RR \t Comp->RR \t Service Time \t First Time \t Last Time \n");
	
	for (int i = 0; i < tot_chunks; i++){
		
		mean_fr_dd += get_lat[i].fr_dedup;
		mean_dd_comp += get_lat[i].dedup_comp;
		mean_comp_rr += get_lat[i].comp_rr;
		mean_dd_rr += get_lat[i].dedup_rr;
		service_time += get_lat[i].last_time - get_lat[i].first_time;
		
		if (get_lat[i].dedup){
			fprintf(lat_file, "%d\t[%d]\t%.3lf\t%s\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",i,get_lat[i].global_id_chunk,get_lat[i].fr_dedup,"isddp",get_lat[i].dedup_rr, get_lat[i].last_time - get_lat[i].first_time , get_lat[i].first_time, get_lat[i].last_time);
		}else{
			
			fprintf(lat_file, "%d\t[%d]\t%.3lf\t%.3lf\t%s\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",i,get_lat[i].global_id_chunk,get_lat[i].fr_dedup,get_lat[i].dedup_comp,"isnotddp",get_lat[i].comp_rr, get_lat[i].last_time - get_lat[i].first_time, get_lat[i].first_time,get_lat[i].last_time);
		}
	}	
	fclose(lat_file);

	mean_fr_dd = mean_fr_dd / tot_chunks;
	mean_dd_comp = mean_dd_comp / current_chunk_not_dd;
	mean_comp_rr = mean_comp_rr / current_chunk_not_dd;
	mean_dd_rr = mean_dd_rr / current_chunk_dd;
	service_time = service_time / tot_chunks;

	sprintf(filename,"%s/latency.dat",path_log);
	lat_file = fopen(filename,"a+");
	
	fprintf(lat_file, "%d\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",nthreads,mean_fr_dd,mean_dd_comp,mean_dd_rr,mean_comp_rr,service_time);
	
	if (UPL_getRealNumCores() == nthreads){
		fprintf(lat_file, "#threads \t FR->Dedup \t Dedup->comp \t Dedup->RR \t Comp->RR \t Service Time \n");
	}

	fclose(lat_file);
	
	sprintf(filename,"%s/real_time_latency_%dth.dat",path_log,nthreads);
	lat_file = fopen(filename,"w");
	
	fprintf(lat_file, "#IDX \t Time Stamp \t FR->Dedup \t Dedup->comp \t Dedup->RR \t Comp->RR \t Service Time \n");
	for (int i = 0; i < number_of_collect_latency; i++){
		
		if (isnan(mean_latency[i].mean_dd_rr)){
			mean_latency[i].mean_dd_rr = 0;
		}

		fprintf(lat_file, "%d\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",i,mean_latency[i].leapse_time, mean_latency[i].mean_fr_dd,mean_latency[i].mean_dd_comp,mean_latency[i].mean_dd_rr,mean_latency[i].mean_comp_rr, mean_latency[i].service_time);
	}
	fclose(lat_file);
	
	sprintf(filename,"%s/real_time_throughput_%dth.dat",path_log,nthreads);
	FILE *THP_file;
	
	THP_file = fopen(filename,"w");
	
	fprintf(THP_file,"#Idex\tTime Stamp\tCurrent_Size\tTime_Spent\tSize_Processed\tThroughput_by_Size\tMemory_Usage\n");
	for (int i = 0; i <= number_of_collect; i++){
		fprintf(THP_file,"%i\t%lf\t%.0lf\t%lf\t%.0lf\t%lf\t%.0lf\n",i,print_metrics[i].time_lapse,print_metrics[i].current_size_processed,print_metrics[i].time_spent,print_metrics[i].total_size_processed,print_metrics[i].throughput_by_size,print_metrics[i].mem_usage);
	}
	fclose(THP_file);

	sprintf(filename,"%s/throughput.dat",path_log);
	
	THP_file = fopen(filename,"a+");

	fprintf(THP_file,"%i\t%.3f\n",nthreads,size_total/execution_time);
	if (UPL_getRealNumCores() == nthreads){
		fprintf(THP_file, "#threads \t Throughput \n");
	}
	fclose(THP_file);

	FILE *plot_metrics;
	
	sprintf(filename,"%s/execution_metrics.dat",path_log);

	plot_metrics = fopen(filename,"a+");

	fprintf(plot_metrics,"%d\t%.3lf\t%.3lf\t%.3lf\t%.3lf\t%.3lf\n",nthreads,execution_time,size_total/execution_time,service_time,mean_chunk_size,stddev_chunk);
	if (UPL_getRealNumCores() == nthreads){
		fprintf(plot_metrics, "#threads \t Execution Time \t Throughput \t Service Time \t Mean Chunk Size \t STDDEV Chunk Size \n");
	}	
	fclose(plot_metrics);
	
	free(get_lat);
	free(mean_latency);
	free(colect_metrics);
}

struct timeval throughput_init_time, throughput_end_time;
double thp_init_time(){
	gettimeofday(&throughput_init_time,NULL);
	return (double)throughput_init_time.tv_sec + (double)throughput_init_time.tv_usec/1e6;
}

double thp_end_time(){
	gettimeofday(&throughput_end_time,NULL);
	return (double)throughput_end_time.tv_sec + (double)throughput_end_time.tv_usec/1e6;
}

double get_utime(){
	struct timeval current_time;	
	gettimeofday(&current_time,NULL);
	double time_now;
	time_now = (double)current_time.tv_sec * (double)1e6 + (double)current_time.tv_usec;
	return time_now;
}

double get_stime(){
	struct timeval current_time;	
	gettimeofday(&current_time,NULL);
	return (double)current_time.tv_sec + (double)current_time.tv_usec/(double)1e6;
}

#endif//ENABLE_TRACING
//######################################

//The configuration block defined in main
config_t * conf;


//Hash table data structure & utility functions

struct hashtable **cache_hash_files;

static unsigned int hash_from_key_fn( void *k ) {
	//NOTE: sha1 sum is integer-aligned
	return ((unsigned int *)k)[0];
}

static int keys_equal_fn ( void *key1, void *key2 ) {
	return (memcmp(key1, key2, SHA1_LEN) == 0);
}

int get_total_files(){

	int check_total_files = 0;
	
	char buff[256];
	sprintf(buff,"ls %s | wc -l",conf->infile);

	char *dat = UPL_getCommandResult(buff);
	return check_total_files = atoi(dat);
}


//Arguments to pass to each thread
struct thread_args {
	//thread id, unique within a thread pool (i.e. unique for a pipeline stage)
	int tid;
	//number of queues available, first and last pipeline stage only
	int nqueues;
	//file descriptor, first pipeline stage only
	int fd;
	//input file buffer, first pipeline stage & preloading only
	struct {
	void *buffer;
	size_t size;
	} input_file;
	struct{
		char * dir_name;
	}dir;
};

#ifdef ENABLE_STATISTICS
//Keep track of block granularity with 2^CHUNK_GRANULARITY_POW resolution (for statistics)
#define CHUNK_GRANULARITY_POW (7)
//Number of blocks to distinguish, CHUNK_MAX_NUM * 2^CHUNK_GRANULARITY_POW is biggest block being recognized (for statistics)
#define CHUNK_MAX_NUM (8*32)
//#define CHUNK_MAX_NUM (64*32)
//Map a chunk size to a statistics array slot
#define CHUNK_SIZE_TO_SLOT(s) ( ((s)>>(CHUNK_GRANULARITY_POW)) >= (CHUNK_MAX_NUM) ? (CHUNK_MAX_NUM)-1 : ((s)>>(CHUNK_GRANULARITY_POW)) )
//Get the average size of a chunk from a statistics array slot
#define SLOT_TO_CHUNK_SIZE(s) ( (s)*(1<<(CHUNK_GRANULARITY_POW)) + (1<<((CHUNK_GRANULARITY_POW)-1)) )
//Deduplication statistics (only used if ENABLE_STATISTICS is defined)

typedef struct {
	/* Cumulative sizes */
	uintmax_t total_input; //Total size of input in bytes
	uintmax_t total_dedup; //Total size of input without duplicate blocks (after global compression) in bytes
	uintmax_t total_compressed; //Total size of input stream after local compression in bytes
	uintmax_t total_output; //Total size of output in bytes (with overhead) in bytes

	/* Size distribution & other properties */
	uintmax_t nChunks[CHUNK_MAX_NUM]; //Coarse-granular size distribution of data chunks
	uintmax_t nDuplicates; //Total number of duplicate blocks

} stats_t;

//Initialize a statistics record
static void init_stats(stats_t *s) {
	int i;

	assert(s!=NULL);
	s->total_input = 0;
	s->total_dedup = 0;
	s->total_compressed = 0;
	s->total_output = 0;

	for(i=0; i<CHUNK_MAX_NUM; i++) {
		s->nChunks[i] = 0;
	}
	s->nDuplicates = 0;
}
#ifdef ENABLE_PTHREADS

//The queues between the pipeline stages
queue_t *deduplicate_que, *refine_que, *reorder_que, *compress_que;

//Merge two statistics records: s1=s1+s2
static void merge_stats(stats_t *s1, stats_t *s2) {
	int i;

	assert(s1!=NULL);
	assert(s2!=NULL);
	s1->total_input += s2->total_input;
	s1->total_dedup += s2->total_dedup;
	s1->total_compressed += s2->total_compressed;
	s1->total_output += s2->total_output;

	for(i=0; i<CHUNK_MAX_NUM; i++) {
		s1->nChunks[i] += s2->nChunks[i];
	}
	s1->nDuplicates += s2->nDuplicates;
}
#endif //ENABLE_PTHREADS

//Print statistics
static void print_stats(stats_t *s) {
	printf("---Print Stats \n");
	const unsigned int unit_str_size = 7; //elements in unit_str array
	const char *unit_str[] = {"Bytes", "KB", "MB", "GB", "TB", "PB", "EB"};
	unsigned int unit_idx = 0;
	uintmax_t unit_div_Tot_In = 1;
	uintmax_t unit_div_Tot_Dedup = 1;
	uintmax_t unit_div_Tot_Comp = 1;
	uintmax_t unit_div_Tot_Out = 1;

	unsigned int UnitTotIn = 0;
	unsigned int UnitTotDedup = 0;
	unsigned int UnitTotComp = 0;
	unsigned int UnitTotOut = 0;

	assert(s!=NULL);
	
	while(unit_idx<unit_str_size){
		uintmax_t unit_div_next = unit_div_Tot_In * 1024;    
		if(s->total_input / unit_div_next <= 0){
			UnitTotIn = unit_idx;
			break;
		}        
		unit_div_Tot_In = unit_div_next;
		unit_idx++;
	}
	
	unit_idx = 0;
	while(unit_idx<unit_str_size){
		uintmax_t unit_div_next = unit_div_Tot_Out * 1024;    
		if(s->total_output / unit_div_next <= 0){
			UnitTotOut = unit_idx;
			break;
		}        
		unit_div_Tot_Out = unit_div_next;
		unit_idx++;
	}

	unit_idx = 0;
	while(unit_idx<unit_str_size){
		uintmax_t unit_div_next = unit_div_Tot_Dedup * 1024;    
		if(s->total_dedup / unit_div_next <= 0){
			UnitTotDedup = unit_idx;
			break;
		}        
		unit_div_Tot_Dedup = unit_div_next;
		unit_idx++;
	}

	unit_idx = 0;
	while(unit_idx<unit_str_size){
		uintmax_t unit_div_next = unit_div_Tot_Comp * 1024;    
		if(s->total_compressed / unit_div_next <= 0){
			UnitTotComp = unit_idx;
			break;
		}        
		unit_div_Tot_Comp = unit_div_next;
		unit_idx++;
	}
	 
	//Total number of chunks
	unsigned int i;
	unsigned int nTotalChunks=0;
	for(i=0; i<CHUNK_MAX_NUM; i++) nTotalChunks+= s->nChunks[i];

	//Average size of chunks
	double mean_size = 0;
	double Tmean_size = 0;
	for(i=0; i<CHUNK_MAX_NUM; i++) mean_size += (double)(SLOT_TO_CHUNK_SIZE(i)) * (double)(s->nChunks[i]);
	Tmean_size = mean_size;
	mean_size = mean_size / (double)nTotalChunks;

	double var_size = 0;
	for(i=0; i<CHUNK_MAX_NUM; i++){
	var_size += (mean_size - (double)(SLOT_TO_CHUNK_SIZE(i))) * (mean_size - (double)(SLOT_TO_CHUNK_SIZE(i))) * (double)(s->nChunks[i]);
	}

	printf("\n##################\n");
	printf("BenSP Metrics\n");
	printf("--------------\n");
	printf("1) File Information \n");
	if (conf->file){
		printf("\tFile name: %s\n", conf->infile);
	}else if(conf->folder){
		printf("\tFolder: %s \n",conf->infile);
		for (int i = 0; i < total_files; i++){
			printf("File[%d]: %s  ",i+1,BSP_chunk[i].BSP.file_name);			
		}
		printf("\n\n");
		free(BSP_chunk);
	}
	
	printf("\tTotal Input Size:\t%.2f %s\n", (float)(s->total_input)/(float)(unit_div_Tot_In), unit_str[UnitTotIn]);
	printf("\tTotal Output Size:\t%.2f %s\n", (float)(s->total_output)/(float)(unit_div_Tot_Out), unit_str[UnitTotOut]);
	printf("\tCompress type (gzip=0, bzip2=1, none=2): %d\n", conf->compress_type);
	
	printf("--------------\n");
	printf("2) Processing Information \n");

	if (conf->preloading == TRUE){
	printf("\tPreloading: ON \n");
	}else{
	printf("\tPreloading: OFF \n");
	}
	
	BSPtime = end_time - init_time;
#ifdef ENABLE_TRACING
	print_time(BSPtime,conf->nthreads);
	print_bensp_metrics(nTotalChunks,colect_metrics,BSPtime,Tmean_size,mean_size,sqrtf(var_size));
#else
	print_time(BSPtime,conf->nthreads);
#endif //ENABLE_TRACING	

	printf("\tTime Processing:\t%lf seconds\n", BSPtime);
	printf("\tEffective compression factor:\t%.2fx\n", (float)(s->total_input)/(float)(s->total_output));
		
	printf("\tTotal Chunks:\t%u\n", nTotalChunks);
	printf("\tSize total of all chunks:\t%.3lf Bytes\n", Tmean_size);
	printf("\tMean data chunk size:\t%.4lf (stddev: %.4lf )\n", mean_size, sqrtf(var_size));
	printf("\tAmount of duplicate chunks:\t%#.2f%% (Amount: %ju)\n", 100.0*(float)(s->nDuplicates)/(float)(nTotalChunks), s->nDuplicates);
	printf("\tData size after deduplication:\t%#.2f %s (compression factor: %.2fx)\n", (float)(s->total_dedup)/(float)(unit_div_Tot_Dedup), unit_str[UnitTotDedup], (float)(s->total_input)/(float)(s->total_dedup));
	printf("\tData size after compression:\t%#.2f %s (compression factor: %.2fx)\n", (float)(s->total_compressed)/(float)(unit_div_Tot_Comp), unit_str[UnitTotComp], (float)(s->total_dedup)/(float)(s->total_compressed));
	printf("\tOutput overhead:\t%#.2f%%\n", 100.0*(float)(s->total_output - s->total_compressed)/(float)(s->total_output));
	printf("\t--------------\n");
		
	printf("\n2.1) Runtime Information\n");
	printf("\tMemory Usage: %ld KB\n", UPL_getProcMemUsage());

	printf("--------------\n");
	printf("3) Parallel Processing Information \n");
	printf("\tNumber of Threads:\t%d\n", conf->nthreads);
	printf("\n3.1) Application Information\n");
	printf("\t--Algorithm Fragmentation\n");
	printf("\tSize Window: %d\n",NWINDOW);
	
	printf("\n\t---Dedup Application\n");
	printf("\tBuffer Fragment Refine Stage: IN: %d OUT: %d\n",IN_BUFFER_FR, OUT_BUFFER_FR);
	printf("\tBuffer Deduplication Stage: IN: %d OUT_COMPRESS: %d OUT_REORDER: %d \n",IN_BUFFER_DD, OUT_BUFFER_DD, IN_BUFFER_RR);
	printf("\tBuffer Compress Stage: IN: %d OUT: %d\n",IN_BUFFER_COMP, OUT_BUFFER_COMP);
	printf("\tBuffer Reorder Stage: IN: %d \n",IN_BUFFER_RR);
		
	printf("--------------\n");
	printf("4) Environment of Test \n");
	printf("\tNumber of Cores %ld (Real Cores %ld) \n", UPL_getNumOfCores(),UPL_getRealNumCores());
	printf("\tNumber of Sockets %ld\n", UPL_getNumSockets());
	printf("\tCpu Frequency %lu\n", UPL_getCpuFreq());
	printf("\tCache Line Size %ld\n", UPL_getCacheLineSize()); 
	
	printf("\t\nSystem Memory Info\n\n");
	printf("%s\n", UPL_getSysMemInfo());

	printf("##################\n\n");

	printf("\n");
	
}

//variable with global statistics
static stats_t stats;
#endif //ENABLE_STATISTICS

//Simple write utility function
static int write_file(int fd, u_char type, u_long len, u_char * content) {
	if (xwrite(fd, &type, sizeof(type)) < 0){
	perror("xwrite:");
	EXIT_TRACE("xwrite type fails\n");
	return -1;
	}
	if (xwrite(fd, &len, sizeof(len)) < 0){
	EXIT_TRACE("xwrite content fails\n");
	}
	if (xwrite(fd, content, len) < 0){
	EXIT_TRACE("xwrite content fails\n");
	}
	return 0;
}

/*
 * Helper function that creates and initializes the output file
 * Takes the file name to use as input and returns the file handle
 * The output file can be used to write chunks without any further steps
 */
static int create_output_file(char *outfile) {
	
	int fd;
	//Create output file
	fd = open(outfile, O_CREAT|O_TRUNC|O_WRONLY|O_TRUNC, S_IRGRP | S_IWUSR | S_IRUSR | S_IROTH);
	if (fd < 0) {
	EXIT_TRACE("Cannot open output file.\n");
	}
	//Write header
	if (write_header(fd, conf->compress_type)) {
	EXIT_TRACE("Cannot write output file header.\n");
	}
	return fd;
}

/*
 * Helper function that writes a chunk to an output file depending on
 * its state. The function will write the SHA1 sum if the chunk has
 * already been written before, or it will write the compressed data
 * of the chunk if it has not been written yet.
 *
 * This function will block if the compressed data is not available yet.
 * This function might update the state of the chunk if there are any changes.
 */
#ifdef ENABLE_PTHREADS
//NOTE: The parallel version checks the state of each chunk to make sure the
//      relevant data is available. If it is not then the function waits.
static void write_chunk_to_file(int fd, chunk_t *chunk) {
	assert(chunk!=NULL);
	//Find original chunk
	if(chunk->header.isDuplicate) chunk = chunk->compressed_data_ref;

	pthread_mutex_lock(&chunk->header.lock);
		while(chunk->header.state == CHUNK_STATE_UNCOMPRESSED) {
		pthread_cond_wait(&chunk->header.update, &chunk->header.lock);
	}

	//state is now guaranteed to be either COMPRESSED or FLUSHED
	if(chunk->header.state == CHUNK_STATE_COMPRESSED) {
	//Chunk data has not been written yet, do so now
	write_file(fd, TYPE_COMPRESS, chunk->compressed_data.n, chunk->compressed_data.ptr);
	mbuffer_free(&chunk->compressed_data);
	chunk->header.state = CHUNK_STATE_FLUSHED;
	} else {
	//Chunk data has been written to file before, just write SHA1
	write_file(fd, TYPE_FINGERPRINT, SHA1_LEN, (unsigned char *)(chunk->sha1));
	}
	pthread_mutex_unlock(&chunk->header.lock);
}
#endif //ENABLE_PTHREADS

int rf_win;
int rf_win_dataprocess;
/*
 * Computational kernel of compression stage
 *
 * Actions performed:
 *  - Compress a data chunk
 */
void sub_Compress(chunk_t *chunk) {
	size_t n;
	int r;

	assert(chunk!=NULL);
	//compress the item and add it to the database
#ifdef ENABLE_PTHREADS
	pthread_mutex_lock(&chunk->header.lock);
	assert(chunk->header.state == CHUNK_STATE_UNCOMPRESSED);
#endif //ENABLE_PTHREADS
	switch (conf->compress_type) {
		case COMPRESS_NONE:
		//Simply duplicate the data
		n = chunk->uncompressed_data.n;
		r = mbuffer_create(&chunk->compressed_data, n);
		if(r != 0) {
			EXIT_TRACE("Creation of compression buffer failed.\n");
		}
		//copy the block
		memcpy(chunk->compressed_data.ptr, chunk->uncompressed_data.ptr, chunk->uncompressed_data.n);
		break;
#ifdef ENABLE_GZIP_COMPRESSION
		case COMPRESS_GZIP:
		//Gzip compression buffer must be at least 0.1% larger than source buffer plus 12 bytes
		n = chunk->uncompressed_data.n + (chunk->uncompressed_data.n >> 9) + 12;
		r = mbuffer_create(&chunk->compressed_data, n);
		if(r != 0) {
			EXIT_TRACE("Creation of compression buffer failed.\n");
		}
		//compress the block
		r = compress(chunk->compressed_data.ptr, &n, chunk->uncompressed_data.ptr, chunk->uncompressed_data.n);
		if (r != Z_OK) {
			EXIT_TRACE("Compression failed\n");
		}
		//Shrink buffer to actual size
		if(n < chunk->compressed_data.n) {
			r = mbuffer_realloc(&chunk->compressed_data, n);
			assert(r == 0);
		}
		break;
#endif //ENABLE_GZIP_COMPRESSION
#ifdef ENABLE_BZIP2_COMPRESSION
		case COMPRESS_BZIP2:
		//Bzip compression buffer must be at least 1% larger than source buffer plus 600 bytes
		n = chunk->uncompressed_data.n + (chunk->uncompressed_data.n >> 6) + 600;
		r = mbuffer_create(&chunk->compressed_data, n);
		if(r != 0) {
			EXIT_TRACE("Creation of compression buffer failed.\n");
		}
		//compress the block
		unsigned int int_n = n;
		r = BZ2_bzBuffToBuffCompress(chunk->compressed_data.ptr, &int_n, chunk->uncompressed_data.ptr, chunk->uncompressed_data.n, 9, 0, 30);
		n = int_n;
		if (r != BZ_OK) {
			EXIT_TRACE("Compression failed\n");
		}
		//Shrink buffer to actual size
		if(n < chunk->compressed_data.n) {
			r = mbuffer_realloc(&chunk->compressed_data, n);
			assert(r == 0);
		}
		break;
#endif //ENABLE_BZIP2_COMPRESSION
		default:
		EXIT_TRACE("Compression type not implemented.\n");
		break;
	}
	mbuffer_free(&chunk->uncompressed_data);

#ifdef ENABLE_PTHREADS
	chunk->header.state = CHUNK_STATE_COMPRESSED;
	pthread_cond_broadcast(&chunk->header.update);
	pthread_mutex_unlock(&chunk->header.lock);
#endif //ENABLE_PTHREADS

	 return;
}

/*
 * Pipeline stage function of compression stage
 *
 * Actions performed:
 *  - Dequeue items from compression queue
 *  - Execute compression kernel for each item
 *  - Enqueue each item into send queue
 */
#ifdef ENABLE_PTHREADS
void *Compress(void * targs) {
	
	struct thread_args *args = (struct thread_args *)targs;
	const int qid = args->tid / MAX_THREADS_PER_QUEUE;
	chunk_t * chunk;
	int r;
	
	ringbuffer_t recv_buf, send_buf;

#ifdef ENABLE_STATISTICS
	stats_t *thread_stats = malloc(sizeof(stats_t));
	if(thread_stats == NULL) EXIT_TRACE("Memory allocation failed.\n");
	init_stats(thread_stats);
#endif //ENABLE_STATISTICS
	
	r=0;
	r += ringbuffer_init(&recv_buf, IN_BUFFER_COMP);
	r += ringbuffer_init(&send_buf, OUT_BUFFER_RR);
	assert(r==0);

	while(1) {
		//get items from the queue
		if (ringbuffer_isEmpty(&recv_buf)) {
			r = queue_dequeue(&compress_que[qid], &recv_buf, IN_BUFFER_COMP);
			if (r < 0) break;
		}

		//fetch one item
		chunk = (chunk_t *)ringbuffer_remove(&recv_buf);
		assert(chunk!=NULL);
	
	#ifdef ENABLE_TRACING
		chunk->dedup_comp = get_utime() - chunk->dedup_comp;
	#endif
	
		sub_Compress(chunk);
	
		thread_stats->total_compressed += chunk->compressed_data.n;	
	
	#ifdef ENABLE_TRACING
		chunk->comp_rr = get_utime();
	#endif
	
		r = ringbuffer_insert(&send_buf, chunk);
		assert(r==0);

		//put the item in the next queue for the write thread
		if (ringbuffer_isFull(&send_buf)) {
			r = queue_enqueue(&reorder_que[qid], &send_buf, OUT_BUFFER_RR);
			assert(r>=1);
		}
	}

	//Enqueue left over items
	while (!ringbuffer_isEmpty(&send_buf)) {
		r = queue_enqueue(&reorder_que[qid], &send_buf, OUT_BUFFER_RR);
		assert(r>=1);
	}

	ringbuffer_destroy(&recv_buf);
	ringbuffer_destroy(&send_buf);

	//shutdown*/
	queue_terminate(&reorder_que[qid]);

	return thread_stats;
}
#endif //ENABLE_PTHREADS

/*
 * Computational kernel of deduplication stage
 *
 * Actions performed:
 *  - Calculate SHA1 signature for each incoming data chunk
 *  - Perform database lookup to determine chunk redundancy status
 *  - On miss add chunk to database
 *  - Returns chunk redundancy status
 */
int sub_Deduplicate(chunk_t *chunk) {
	int isDuplicate;
	chunk_t *entry;

	assert(chunk!=NULL);
	assert(chunk->uncompressed_data.ptr!=NULL);
	SHA1_Digest(chunk->uncompressed_data.ptr, chunk->uncompressed_data.n, (unsigned char *)(chunk->sha1));
	//Query database to determine whether we've seen the data chunk before
#ifdef ENABLE_PTHREADS
	//pthread_mutex_t *ht_lock = hashtable_getlock(cache, (void *)(chunk->sha1));
	pthread_mutex_t *ht_lock = hashtable_getlock(cache_hash_files[chunk->BSP.idx_file], (void *)(chunk->sha1));
	pthread_mutex_lock(ht_lock);
#endif
	entry = (chunk_t *)hashtable_search(cache_hash_files[chunk->BSP.idx_file], (void *)(chunk->sha1));
	isDuplicate = (entry != NULL);
	chunk->header.isDuplicate = isDuplicate;
	if (!isDuplicate) {
	// Cache miss: Create entry in hash table and forward data to compression stage
#ifdef ENABLE_PTHREADS
	pthread_mutex_init(&chunk->header.lock, NULL);
	pthread_cond_init(&chunk->header.update, NULL);
#endif
	//NOTE: chunk->compressed_data.buffer will be computed in compression stage
	if (hashtable_insert(cache_hash_files[chunk->BSP.idx_file], (void *)(chunk->sha1), (void *)chunk) == 0) {
		EXIT_TRACE("hashtable_insert failed");
	}
	} else {
	// Cache hit: Skipping compression stage
	chunk->compressed_data_ref = entry;
	mbuffer_free(&chunk->uncompressed_data);
	}
#ifdef ENABLE_PTHREADS
	pthread_mutex_unlock(ht_lock);
#endif
	return isDuplicate;
}

/*
 * Pipeline stage function of deduplication stage
 *
 * Actions performed:
 *  - Take input data from fragmentation stages
 *  - Execute deduplication kernel for each data chunk
 *  - Route resulting package either to compression stage or to reorder stage, depending on deduplication status
 */
#ifdef ENABLE_PTHREADS
void * Deduplicate(void * targs) {
	
	struct thread_args *args = (struct thread_args *)targs;
	const int qid = args->tid / MAX_THREADS_PER_QUEUE;
	chunk_t *chunk;
	int r;
	
	ringbuffer_t recv_buf, send_buf_reorder, send_buf_compress;

	stats_t *thread_stats = malloc(sizeof(stats_t));
	if(thread_stats == NULL) {
		EXIT_TRACE("Memory allocation failed.\n");
	}
	init_stats(thread_stats);
	
	r=0;
	r += ringbuffer_init(&recv_buf, IN_BUFFER_DD);
	r += ringbuffer_init(&send_buf_reorder, OUT_BUFFER_RR);
	r += ringbuffer_init(&send_buf_compress, OUT_BUFFER_COMP);
	assert(r==0);
	
	while (1) {
	//if no items available, fetch a group of items from the queue
		if (ringbuffer_isEmpty(&recv_buf)) {
			r = queue_dequeue(&deduplicate_que[qid], &recv_buf, IN_BUFFER_DD);
			if (r < 0){
			break;
			}
		}

		//get one chunk
		chunk = (chunk_t *)ringbuffer_remove(&recv_buf);
		assert(chunk!=NULL);
	#ifdef ENABLE_TRACING
		chunk->fr_dedup = get_utime() - chunk->first_time;
	#endif
		//Do the processing*/
		int isDuplicate = sub_Deduplicate(chunk);
	
		if(isDuplicate) {
			thread_stats->nDuplicates++;
		} else {
			thread_stats->total_dedup += chunk->uncompressed_data.n;
		}

		//Enqueue chunk either into compression queue or into send queue
		if(!isDuplicate) {
	#ifdef ENABLE_TRACING	
			chunk->dedup_comp = get_utime();
	#endif
			r = ringbuffer_insert(&send_buf_compress, chunk);
			assert(r==0);
			if (ringbuffer_isFull(&send_buf_compress)) {
				r = queue_enqueue(&compress_que[qid], &send_buf_compress, OUT_BUFFER_COMP);
				assert(r>=1);
			}
		} else {
	#ifdef ENABLE_TRACING
			chunk->dedup_rr = get_utime();
	#endif
			r = ringbuffer_insert(&send_buf_reorder, chunk);
			assert(r==0);
			if (ringbuffer_isFull(&send_buf_reorder)) {
				r = queue_enqueue(&reorder_que[qid], &send_buf_reorder, OUT_BUFFER_RR);
				assert(r>=1);
			}
		}
	}

	//empty buffers
	while(!ringbuffer_isEmpty(&send_buf_compress)) {
		r = queue_enqueue(&compress_que[qid], &send_buf_compress, OUT_BUFFER_COMP);
		assert(r>=1);
	}
	while(!ringbuffer_isEmpty(&send_buf_reorder)) {
		r = queue_enqueue(&reorder_que[qid], &send_buf_reorder, OUT_BUFFER_RR);
		assert(r>=1);
	}

	ringbuffer_destroy(&recv_buf);
	ringbuffer_destroy(&send_buf_compress);
	ringbuffer_destroy(&send_buf_reorder);

	//shutdown*/
	queue_terminate(&compress_que[qid]);
	
	return thread_stats;
}
#endif //ENABLE_PTHREADS

void *Fragment(void * targs) {
	
	struct thread_args *args = (struct thread_args *)targs;
	size_t preloading_buffer_seek;
	int qid = 0;
	int32 fd;
	int i;

	char path_file[256];
	int count_files = 0;
	
	ringbuffer_t send_buf;
	sequence_number_t anchorcount = 0;
	int r;

	chunk_t *chunk = NULL;
	
	r = ringbuffer_init(&send_buf, ANCHOR_DATA_PER_INSERT);
	assert(r==0);
	
	size_t bytes_read;
	
	void *file_loading = NULL;
	get_file_t *files = NULL;
	
	DIR *folder;
	struct dirent *in_file;
	struct stat file;
	
	if (NULL == (folder = opendir(args->dir.dir_name)) ){
		EXIT_TRACE("Error open dir.\n");
	}
	
	files = (get_file_t*)malloc(how_many_files*sizeof(get_file_t));
	if (files == NULL){
		EXIT_TRACE("Error allocating memory for input buffer.\n");  
	}
		
	while( (in_file = readdir(folder)) ){
		
		if (!strcmp(in_file->d_name,".")){
			continue;
		} 
		if (!strcmp(in_file->d_name,"..")){
			continue;
		}
		if ( ((strchr(in_file->d_name,'.')) - in_file->d_name+1) == 1 ){
			continue;
		}
		
		preloading_buffer_seek = 0;
		files[count_files].filename = in_file->d_name;

		sprintf(path_file,"%s/%s",conf->infile,files[count_files].filename);
		
		if (stat(path_file,&file) == -1){
			EXIT_TRACE("stat() %s failed: %s\n", path_file, strerror(errno));
		}

		files[count_files].size_file = file.st_size;
		stats.total_input += files[count_files].size_file;

		if((fd = open(path_file, O_RDONLY | O_LARGEFILE)) < 0) 
		EXIT_TRACE("%s file open error %s\n", path_file, strerror(errno));

		file_loading = malloc(files[count_files].size_file);
		if (file_loading == NULL){
			EXIT_TRACE("Error allocating memory for input buffer.\n");
		}

		cache_hash_files[count_files] = hashtable_create(65536, hash_from_key_fn, keys_equal_fn, FALSE);
		conf->preloading = TRUE;

		bytes_read = 0;
		while(bytes_read < files[count_files].size_file){
			r = read(fd, file_loading+bytes_read,files[count_files].size_file-bytes_read);
			if(r<0) switch(errno) {
				case EAGAIN:
					EXIT_TRACE("I/O error: No data available\n");break;
				case EBADF:
					EXIT_TRACE("I/O error: Invalid file descriptor\n");break;
				case EFAULT:
					EXIT_TRACE("I/O error: Buffer out of range\n");break;
				case EINTR:
					EXIT_TRACE("I/O error: Interruption\n");break;
				case EINVAL:
					EXIT_TRACE("I/O error: Unable to read from file descriptor\n");break;
				case EIO:
					EXIT_TRACE("I/O error: Generic I/O error\n");break;
				case EISDIR:
					EXIT_TRACE("I/O error: Cannot read from a directory\n");break;
				default:
					EXIT_TRACE("I/O error: Unrecognized error\n");break;
			}
			if(r==0) break;
			bytes_read += r;
		}

		size_t bytes_left = 0; //amount of data left over in last_mbuffer from previous iteration
		while (1) {

			if(ringbuffer_isFull(&send_buf)) {
				r = queue_enqueue(&refine_que[qid], &send_buf, ANCHOR_DATA_PER_INSERT);
				assert(r>=1);
				qid = (qid+1) % args->nqueues;
			}
						
			//Make sure that system supports new buffer size
			if(MAXBUF+bytes_left > SSIZE_MAX) {
				EXIT_TRACE("Input buffer size exceeds system maximum.\n");
			}
			//Allocate a new chunk and create a new memory buffer
			chunk = (chunk_t *)malloc(sizeof(chunk_t));
			if(chunk==NULL) EXIT_TRACE("Memory allocation failed.\n");
			
			r = mbuffer_create(&chunk->uncompressed_data, files[count_files].size_file);
			if(r!=0) {
				EXIT_TRACE("Unable to initialize memory buffer.\n");
			}

			bytes_read=0;
			memcpy(chunk->uncompressed_data.ptr, file_loading+preloading_buffer_seek, files[count_files].size_file);
			bytes_read = files[count_files].size_file;
			preloading_buffer_seek += files[count_files].size_file;						

			if(bytes_read == files[count_files].size_file){
				chunk->BSP.idx_file = count_files;
				chunk->BSP.file_name = files[count_files].filename;
				chunk->BSP.file_size = files[count_files].size_file;
				chunk->BSP.L1_anchorcount = anchorcount;
				chunk->BSP.FLAG_EOF = BSP_EOF;
				chunk->header.state = CHUNK_STATE_UNCOMPRESSED;
				chunk->sequence.l1num = anchorcount;	
				
				BSP_chunk[count_files].BSP.idx_file = count_files;
				BSP_chunk[count_files].BSP.file_name = files[count_files].filename;
				BSP_chunk[count_files].BSP.L1_anchorcount = anchorcount;
				BSP_chunk[count_files].BSP.file_size = files[count_files].size_file;
				anchorcount++;
				
				//put it into send buffer
				r = ringbuffer_insert(&send_buf, chunk);
				assert(r==0);
				break;
			}
		}
		
		if(ringbuffer_isFull(&send_buf)) {
			r = queue_enqueue(&refine_que[qid], &send_buf, ANCHOR_DATA_PER_INSERT);
			assert(r>=1);
			qid = (qid+1) % args->nqueues;
		}
				
		free(file_loading);

		if (files[count_files].filename != NULL){
			close(fd);
		}
		
		count_files++;

		if (how_many_files < count_files){
			break;
		}
	}//End While get files
	
	while(!ringbuffer_isEmpty(&send_buf)) {
		r = queue_enqueue(&refine_que[qid], &send_buf, ANCHOR_DATA_PER_INSERT);
		assert(r>=1);
		qid = (qid+1) % args->nqueues;
	}

	free(files);
	
	ringbuffer_destroy(&send_buf);
	total_files = count_files;
	
	for(i=0; i<args->nqueues; i++) {
		queue_terminate(&refine_que[i]);
	}
	
	return NULL;
}

#ifdef ENABLE_PTHREADS
void *FragmentRefine(void * targs) {
	
	struct thread_args *args = (struct thread_args *)targs;
	const int qid = args->tid / MAX_THREADS_PER_QUEUE;
	ringbuffer_t recv_buf, send_buf;
	int r;
	
	int count_files = 0;
	sequence_number_t chcount = 0;

	chunk_t *temp;
	chunk_t *chunk;

	u32int * rabintab = malloc(256*sizeof rabintab[0]);
	u32int * rabinwintab = malloc(256*sizeof rabintab[0]);
	if(rabintab == NULL || rabinwintab == NULL) {
		EXIT_TRACE("Memory allocation failed.\n");
	}

	r=0;
	r += ringbuffer_init(&recv_buf, IN_BUFFER_FR);
	r += ringbuffer_init(&send_buf, OUT_BUFFER_FR);
	assert(r==0);

	stats_t *thread_stats = malloc(sizeof(stats_t));
	if(thread_stats == NULL) {
		EXIT_TRACE("Memory allocation failed.\n");
	}
	init_stats(thread_stats);
	
	while(!ringbuffer_isEmpty(&recv_buf)){
		usleep(10);
	}

	while (TRUE) {
				
		if (ringbuffer_isEmpty(&recv_buf)) {
			r = queue_dequeue(&refine_que[qid], &recv_buf, IN_BUFFER_FR);
			assert(r>=1);
		}
		
		//get one item
		chunk = (chunk_t *)ringbuffer_remove(&recv_buf);
		assert(chunk!=NULL);

		rabininit(rf_win, rabintab, rabinwintab);
		
		count_files = chunk->BSP.idx_file;

		int split;
		chcount = 0;
		
		do {						
			//Find next anchor with Rabin fingerprint		
			int offset = rabinseg(chunk->uncompressed_data.ptr, chunk->uncompressed_data.n, rf_win, rabintab, rabinwintab);
			
			if(offset < chunk->uncompressed_data.n) {			
				//Allocate a new chunk and create a new memory buffer
				temp = (chunk_t *)malloc(sizeof(chunk_t));
				if(temp==NULL) EXIT_TRACE("Memory allocation failed.\n");
				temp->header.state = chunk->header.state;
				temp->sequence.l1num = chunk->sequence.l1num;
				temp->BSP.idx_file = count_files;

				//split it into two pieces
				r = mbuffer_split(&chunk->uncompressed_data, &temp->uncompressed_data, offset);
				if(r!=0) EXIT_TRACE("Unable to split memory buffer.\n");

				//Set correct state and sequence numbers
				chunk->BSP.idx_file = count_files;
				chunk->sequence.l2num = chcount;
				chunk->isLastL2Chunk = FALSE;
				chunk->current_global_id = global_id;
				chcount++;
				global_id++;
	
				thread_stats->nChunks[CHUNK_SIZE_TO_SLOT(chunk->uncompressed_data.n)]++;
	
				if (ringbuffer_isFull(&send_buf)) {
					r = queue_enqueue(&deduplicate_que[qid], &send_buf, OUT_BUFFER_FR);
					assert(r>=1);
				}

			#ifdef ENABLE_TRACING
				chunk->first_time = get_utime();
			#endif

				r = ringbuffer_insert(&send_buf, chunk);
				assert(r==0);

				//prepare for next iteration
				chunk = temp;
				split = 1;
			} else {
				//End of buffer reached, don't split but simply enqueue it
				//Set correct state and sequence numbers
				chunk->sequence.l2num = chcount;
				chunk->isLastL2Chunk = TRUE;
				chcount++;
				chunk->BSP.idx_file = count_files;
				
				chunk->current_global_id = global_id;
				global_id++;
				
				thread_stats->nChunks[CHUNK_SIZE_TO_SLOT(chunk->uncompressed_data.n)]++;
				
				if (ringbuffer_isFull(&send_buf)) {
					r = queue_enqueue(&deduplicate_que[qid], &send_buf, OUT_BUFFER_FR);
					assert(r>=1);
				}
			#ifdef ENABLE_TRACING
				chunk->first_time = get_utime();
			#endif
				r = ringbuffer_insert(&send_buf, chunk);
				assert(r==0);
				
				while(!ringbuffer_isEmpty(&send_buf)){
					r = queue_enqueue(&deduplicate_que[qid], &send_buf, OUT_BUFFER_FR);
					assert(r>=1);
				}

				//prepare for next iteration
				chunk = NULL;
				split = 0;
			}

		}while(split);
		
		BSP_chunk[count_files].BSP.L2_chcount = chcount;
		
		if (ringbuffer_isEmpty(&recv_buf)) {
			r = queue_dequeue(&refine_que[qid], &recv_buf, IN_BUFFER_FR);
			if (r < 0) {
				break;
			}
		}
	}
	while(!ringbuffer_isEmpty(&send_buf)) {
		r = queue_enqueue(&deduplicate_que[qid], &send_buf, OUT_BUFFER_FR);
		assert(r>=1);
	}
	
	free(rabintab);
	free(rabinwintab);
	ringbuffer_destroy(&recv_buf);
	ringbuffer_destroy(&send_buf);

	//shutdown
	queue_terminate(&deduplicate_que[qid]);

	return thread_stats;
}
#endif //ENABLE_PTHREADS

#ifdef ENABLE_PTHREADS
void *Reorder(void * targs) {
	
	int count_files = 0;
	struct stat file;
	struct thread_args *args = (struct thread_args *)targs;
	int qid = 0;
	int fd = 0;

	int l1_chunk = 0;
	int l2_chunk = 0;
	int current_chunk = 0;
	int tot_chunks = 0;

#ifdef ENABLE_TRACING
	colect_metrics = malloc(600*sizeof(metrics_t));
	if (colect_metrics == NULL){
		EXIT_TRACE("Error allocating memory for metrics struct");
	}

	int initial_size = 1000000;
	
	get_lat = malloc(initial_size*sizeof(latency));
	mean_latency = malloc(600*sizeof(latency));

	if (get_lat == NULL && mean_latency == NULL) EXIT_TRACE("Error allocating memory for latency metrics ");

	int all_chunks_in_reorder = 0;
	int last_chunk_dd = 0;
	int last_chunk_not_dd = 0;
	int last_chunk_processed = 0;
	int chunk_dd_processed = 0; 
	int chunk_not_dd_processed = 0; 
	int chunk_processed = 0;

	double mean_fr_dd = 0;
  	double mean_dd_comp = 0 ;
  	double mean_comp_rr = 0;
  	double mean_dd_rr = 0;
  	double service_time = 0;

	double time_now_latency = 0;
	double baseline_time_latency = 0;

	double current_size_processed = 0;
	double last_size_processed = 0;
	
	double baseline_time = 0;
	double time_now = 0;
	double threshold_time = 1;
	
#endif//end tracing	
	
	ringbuffer_t recv_buf;
	chunk_t *chunk;
	
	char out[256];

	SearchTree T;
	T = TreeMakeEmpty(NULL);
	Position pos = NULL;
	struct tree_element tele;

	sequence_t next;
	sequence_reset(&next);

	//We perform global anchoring in the first stage and refine the anchoring
	//in the second stage. This array keeps track of the number of chunks in
	//a coarse chunk.

	sequence_number_t *chunks_per_anchor;
	unsigned int chunks_per_anchor_max = 1024;
	chunks_per_anchor = malloc(chunks_per_anchor_max * sizeof(sequence_number_t));
	if(chunks_per_anchor == NULL) EXIT_TRACE("Error allocating memory\n");
	memset(chunks_per_anchor, 0, chunks_per_anchor_max * sizeof(sequence_number_t));
	
	int r;
	int i;
	r = ringbuffer_init(&recv_buf, IN_BUFFER_RR);
	assert(r==0);
		
	while(ringbuffer_isEmpty(&recv_buf)){
		for(i=0,r=0; r<=0 && i<args->nqueues; i++) {
			r = queue_dequeue(&reorder_que[qid], &recv_buf, IN_BUFFER_RR);
			qid = (qid+1) % args->nqueues;
		}
		if(r<0) break;
	}
	if (count_files == 0){
		sprintf(out,"%s%s%s",conf->outfile,BSP_chunk[count_files].BSP.file_name,".ddp");
		fd = create_output_file(out);
		count_files++;
	}
#ifdef ENABLE_TRACING
	
	//BenSP Comment: Baseline time para o cálculo de throughput e latencia.
	baseline_time = thp_init_time();
	baseline_time_latency = thp_init_time();

	colect_metrics[number_of_collect].time_lapse = get_stime() - init_time;
	colect_metrics[number_of_collect].current_size_processed = current_size_processed;
#endif//END ENABLE_TRACING	
	
	while(1) {
		
		if (ringbuffer_isEmpty(&recv_buf)) {
			for(i=0,r=0; r<=0 && i<args->nqueues; i++) {
				r = queue_dequeue(&reorder_que[qid], &recv_buf, IN_BUFFER_RR);
				qid = (qid+1) % args->nqueues;
			}
			if(r<0) break;
		}

		chunk = (chunk_t *)ringbuffer_remove(&recv_buf);
		if (chunk == NULL) break;

#ifdef ENABLE_TRACING

		chunk->last_time = get_utime();
		time_now_latency = thp_init_time();
		
		if (initial_size <= (all_chunks_in_reorder-1) ){
			
			int size_realloc = initial_size + initial_size/2;
			
			realloc_get_latency = realloc(get_lat,size_realloc*sizeof(latency));			
			if (realloc_get_latency == NULL) EXIT_TRACE("Error reallocating memory for latency metric chunks\n");
						
			get_lat = realloc_get_latency;
			initial_size = size_realloc;
		}

		//BenSP: Pegando tempo de latência
		get_lat[all_chunks_in_reorder].id_file = chunk->sequence.l1num;
		get_lat[all_chunks_in_reorder].global_id_chunk = chunk->current_global_id;
		get_lat[all_chunks_in_reorder].first_time = chunk->first_time;
		get_lat[all_chunks_in_reorder].fr_dedup = chunk->fr_dedup;
		mean_fr_dd += chunk->fr_dedup;
		service_time += chunk->last_time - chunk->first_time;
		
		if (chunk->header.isDuplicate){
			current_chunk_dd++;
			get_lat[all_chunks_in_reorder].dedup_rr = chunk->last_time - chunk->dedup_rr;
			mean_dd_rr += chunk->last_time - chunk->dedup_rr;
			get_lat[all_chunks_in_reorder].dedup = 1;
			get_lat[all_chunks_in_reorder].last_time = chunk->last_time;
		}else{
			current_chunk_not_dd++;
			get_lat[all_chunks_in_reorder].dedup_comp = chunk->dedup_comp;
			mean_dd_comp += chunk->dedup_comp;
			get_lat[all_chunks_in_reorder].comp_rr = chunk->last_time - chunk->comp_rr;
			mean_comp_rr += chunk->last_time - chunk->comp_rr;
			get_lat[all_chunks_in_reorder].dedup = 0;
			get_lat[all_chunks_in_reorder].last_time = chunk->last_time;
		}
		all_chunks_in_reorder++;

		if ( ( time_now_latency - threshold_time) >= baseline_time_latency){
			
			chunk_dd_processed = current_chunk_dd - last_chunk_dd;
			chunk_not_dd_processed = current_chunk_not_dd - last_chunk_not_dd;
			chunk_processed = all_chunks_in_reorder - last_chunk_processed;

			mean_latency[number_of_collect_latency].mean_fr_dd = mean_fr_dd / (chunk_dd_processed+chunk_not_dd_processed);
			mean_latency[number_of_collect_latency].mean_dd_comp = mean_dd_comp / chunk_not_dd_processed;
			mean_latency[number_of_collect_latency].mean_comp_rr = mean_comp_rr / chunk_not_dd_processed;
			mean_latency[number_of_collect_latency].mean_dd_rr = mean_dd_rr / chunk_dd_processed;
			mean_latency[number_of_collect_latency].service_time = service_time /chunk_processed;
			mean_latency[number_of_collect_latency].leapse_time = get_stime() - init_time;
			
			mean_fr_dd = 0;
			mean_dd_comp = 0;
			mean_comp_rr = 0;
			mean_dd_rr = 0;
			last_chunk_dd = current_chunk_dd;
			last_chunk_not_dd = current_chunk_not_dd;
			last_chunk_processed = all_chunks_in_reorder;
			number_of_collect_latency++;
			baseline_time_latency = thp_init_time();
		}

#endif//END ENABLE_TRACING	

		//Double size of sequence number array if necessary
		if(chunk->sequence.l1num >= chunks_per_anchor_max) {
			chunks_per_anchor = realloc(chunks_per_anchor, 2 * chunks_per_anchor_max * sizeof(sequence_number_t));
			if(chunks_per_anchor == NULL) EXIT_TRACE("Error allocating memory\n");
			memset(&chunks_per_anchor[chunks_per_anchor_max], 0, chunks_per_anchor_max * sizeof(sequence_number_t));
			chunks_per_anchor_max *= 2;
		}
		//Update expected L2 sequence number
		if(chunk->isLastL2Chunk) {
			assert(chunks_per_anchor[chunk->sequence.l1num] == 0);
			chunks_per_anchor[chunk->sequence.l1num] = chunk->sequence.l2num+1;			
			l1_chunk++;
		}
		//Put chunk into local cache if it's not next in the sequence 
		if(!sequence_eq(chunk->sequence, next)) {
			pos = TreeFind(chunk->sequence.l1num, T);
			if (pos == NULL) {
				//FIXME: Can we remove at least one of the two mallocs in this if-clause?
				//FIXME: Rename "INITIAL_SEARCH_TREE_SIZE" to something more accurate
				tele.l1num = chunk->sequence.l1num;
				tele.queue = Initialize(INITIAL_SEARCH_TREE_SIZE);
				Insert(chunk, tele.queue);
				T = TreeInsert(tele, T);
			}else{
				Insert(chunk, pos->Element.queue);
			}
			continue;
		}

		//write as many chunks as possible, current chunk is next in sequence
		pos = TreeFindMin(T);
		do {

#ifdef ENABLE_TRACING
			time_now = thp_end_time();
			if ( (time_now - threshold_time) >= baseline_time ){
			
				double leapse_time = (time_now - baseline_time);

				number_of_collect++;

				colect_metrics[number_of_collect].time_lapse = get_stime() - init_time;
				colect_metrics[number_of_collect].time_spent = leapse_time;
				
				colect_metrics[number_of_collect].mem_usage = (double)UPL_getProcMemUsage();

				last_size_processed = current_size_processed - last_size_processed;
				colect_metrics[number_of_collect].throughput_by_size = last_size_processed/leapse_time;

				colect_metrics[number_of_collect].current_size_processed = current_size_processed;
				colect_metrics[number_of_collect].total_size_processed = last_size_processed;

				last_size_processed = current_size_processed;
				
				baseline_time = thp_init_time();
			}

			current_size_processed += chunk->compressed_data.n;
#endif //ENABLE_TRACING
			
			current_chunk++;
			l2_chunk++;
			
			write_chunk_to_file(fd, chunk);
			
			if(chunk->header.isDuplicate) {
			
			#ifdef ENABLE_TRACING
				current_size_processed += chunk->compressed_data_ref->compressed_data.n;
			#endif //ENABLE_TRACING				
				free(chunk);
				chunk=NULL;
			}			
			
			sequence_inc_l2(&next);
			
			if(chunks_per_anchor[next.l1num]!=0 && next.l2num==chunks_per_anchor[next.l1num]){
				
				if (BSP_chunk[count_files].BSP.file_name != NULL){
				
				#ifdef ENABLE_TRACING
					current_size_processed += chunk->compressed_data.n;
				#endif //ENABLE_TRACING						
				
					write_chunk_to_file(fd, chunk);				
					
					if(chunk->header.isDuplicate) {
				
				#ifdef ENABLE_TRACING						
						current_size_processed += chunk->compressed_data_ref->compressed_data.n;
				#endif //ENABLE_TRACING					
						free(chunk);
						chunk=NULL;
					}

					close(fd);
					
					if (stat(out, &file) < 0)
					EXIT_TRACE("stat() %s failed: %s\n", out, strerror(errno));
					
					stats.total_output += file.st_size;
					
					sprintf(out,"%s%s%s",conf->outfile,BSP_chunk[count_files].BSP.file_name,".ddp");
					fd = create_output_file(out);
					count_files++;
				}
				
				tot_chunks += l2_chunk;
				l2_chunk = 0;
				sequence_inc_l1(&next);	
			} 
			
			//Check whether we can write more chunks from cache
			if(pos != NULL && (pos->Element.l1num == next.l1num)) {
				chunk = FindMin(pos->Element.queue);
				if(sequence_eq(chunk->sequence, next)) {
					//Remove chunk from cache, update position for next iteration
					DeleteMin(pos->Element.queue);
					if(IsEmpty(pos->Element.queue)) {
						Destroy(pos->Element.queue);
						T = TreeDelete(pos->Element, T);
						pos = TreeFindMin(T);
					}
				}else{
					//level 2 sequence number does not match
					chunk = NULL;
				}
			}else{
				//level 1 sequence number does not match or no chunks left in cache
				chunk = NULL;
			}

		}while(chunk != NULL);

#ifdef ENABLE_TRACING
		time_now = thp_end_time();
		if ( (time_now - threshold_time) >= baseline_time  ){
		
			double leapse_time = (time_now - baseline_time);

			number_of_collect++;

			colect_metrics[number_of_collect].time_lapse = get_stime() - init_time;
			colect_metrics[number_of_collect].time_spent = leapse_time;
			colect_metrics[number_of_collect].mem_usage = (double)UPL_getProcMemUsage();
			
			last_size_processed = current_size_processed - last_size_processed;
			
			colect_metrics[number_of_collect].throughput_by_size = last_size_processed/leapse_time;
			colect_metrics[number_of_collect].current_size_processed = current_size_processed;
			colect_metrics[number_of_collect].total_size_processed = last_size_processed;
			
			last_size_processed = current_size_processed;
			
			baseline_time = thp_init_time();
		}
#endif //ENABLE_TRACING
	}

	//flush the blocks left in the cache to file
	pos = TreeFindMin(T);
	while(pos !=NULL) {
		if(pos->Element.l1num == next.l1num) {
			chunk = FindMin(pos->Element.queue);
			if(sequence_eq(chunk->sequence, next)) {
			//Remove chunk from cache, update position for next iteration
			DeleteMin(pos->Element.queue);
			if(IsEmpty(pos->Element.queue)){
				Destroy(pos->Element.queue);
				T = TreeDelete(pos->Element, T);
				pos = TreeFindMin(T);
			}
			} else {
			//level 2 sequence number does not match
			EXIT_TRACE("L2 sequence number mismatch.\n");
			}
		} else {
			//level 1 sequence number does not match
			EXIT_TRACE("L1 sequence number mismatch.\n");
		}

	#ifdef ENABLE_TRACING		
		current_size_processed += chunk->compressed_data.n;
	#endif //ENABLE_TRACING

		write_chunk_to_file(fd, chunk);

		if(chunk->header.isDuplicate) {
		
		#ifdef ENABLE_TRACING
			current_size_processed += chunk->compressed_data_ref->compressed_data.n;
		#endif //ENABLE_TRACING			
		
			free(chunk);
			chunk=NULL;		
		}
		
		sequence_inc_l2(&next);
		if(chunks_per_anchor[next.l1num]!=0 && next.l2num==chunks_per_anchor[next.l1num]) sequence_inc_l1(&next);
	}
	
	if (stat(out, &file) < 0) 
		EXIT_TRACE("stat() %s failed: %s\n", out, strerror(errno));

	stats.total_output += file.st_size;
	
	close(fd);

	ringbuffer_destroy(&recv_buf);
	free(chunks_per_anchor);

#ifdef ENABLE_TRACING
	time_now = thp_end_time();
	if ( (time_now - threshold_time) <= baseline_time ){

		double leapse_time = (time_now - baseline_time);

		number_of_collect++;

		colect_metrics[number_of_collect].time_lapse = get_stime() - init_time;
		colect_metrics[number_of_collect].time_spent = leapse_time;
		colect_metrics[number_of_collect].mem_usage = (double)UPL_getProcMemUsage();

		last_size_processed = current_size_processed - last_size_processed;

		colect_metrics[number_of_collect].throughput_by_size = last_size_processed/leapse_time;
		colect_metrics[number_of_collect].current_size_processed = current_size_processed;
		colect_metrics[number_of_collect].total_size_processed = last_size_processed;			
	}

	time_now_latency = thp_init_time();
	if ( ( time_now_latency - threshold_time) <= baseline_time_latency){
			
		chunk_dd_processed = current_chunk_dd - last_chunk_dd;
		chunk_not_dd_processed = current_chunk_not_dd - last_chunk_not_dd;
		chunk_processed = all_chunks_in_reorder - last_chunk_processed;				

		mean_latency[number_of_collect_latency].mean_fr_dd = mean_fr_dd / (chunk_dd_processed+chunk_not_dd_processed);
		mean_latency[number_of_collect_latency].mean_dd_comp = mean_dd_comp / chunk_not_dd_processed;
		mean_latency[number_of_collect_latency].mean_comp_rr = mean_comp_rr / chunk_not_dd_processed;
		mean_latency[number_of_collect_latency].mean_dd_rr = mean_dd_rr / chunk_dd_processed;
		mean_latency[number_of_collect_latency].service_time = service_time /chunk_processed;
		mean_latency[number_of_collect_latency].leapse_time = get_stime() - init_time;

		number_of_collect_latency++;
	}

	nthreads = conf->nthreads;
#endif //ENABLE_TRACING
	
	return NULL;
}
#endif //ENABLE_PTHREADS

/*--------------------------------------------------------------------------*/
/* Encode
 * Compress an input stream
 *
 * Arguments:
 *   conf:    Configuration parameters
 *
 */
void Encode(config_t * _conf) {
	struct stat filestat;
	int32 fd;
	conf = _conf;
	void *preloading_buffer = NULL;   

	init_stats(&stats);

#ifdef ENABLE_PTHREADS
	struct thread_args data_process_args;
	int i;

	const int nqueues = (conf->nthreads / MAX_THREADS_PER_QUEUE) + ((conf->nthreads % MAX_THREADS_PER_QUEUE != 0) ? 1 : 0);
	
	deduplicate_que = malloc(sizeof(queue_t) * nqueues);
	refine_que = malloc(sizeof(queue_t) * nqueues);
	reorder_que = malloc(sizeof(queue_t) * nqueues);
	compress_que = malloc(sizeof(queue_t) * nqueues);
	
	if( (deduplicate_que == NULL) || (refine_que == NULL) || (reorder_que == NULL) || (compress_que == NULL)) {
	printf("Out of memory\n");
	exit(1);
	}
	
	int threads_per_queue;
	for(i=0; i<nqueues; i++) {
	if (i < nqueues -1 || conf->nthreads %MAX_THREADS_PER_QUEUE == 0) {
		//all but last queue
		threads_per_queue = MAX_THREADS_PER_QUEUE;
	} else {
		//remaining threads work on last queue
		threads_per_queue = conf->nthreads %MAX_THREADS_PER_QUEUE;
	}

	//call queue_init with threads_per_queue
	queue_init(&deduplicate_que[i], QUEUE_SIZE, threads_per_queue);
	queue_init(&refine_que[i], QUEUE_SIZE, 1);
	queue_init(&reorder_que[i], QUEUE_SIZE, threads_per_queue);
	queue_init(&compress_que[i], QUEUE_SIZE, threads_per_queue);
	}
#else
	struct thread_args generic_args;
#endif //ENABLE_PTHREADS

	assert(!mbuffer_system_init());

	/* src file stat */
	if (stat(conf->infile, &filestat) < 0){
		EXIT_TRACE("stat() %s failed: %s\n", conf->infile, strerror(errno));
	}

	if (S_ISDIR(filestat.st_mode)){
		
		data_process_args.dir.dir_name = conf->infile;
		conf->folder = TRUE;
		conf->file = FALSE;
		
		how_many_files = get_total_files();
		//cache_hash_files = malloc(SOURCE_FILES*sizeof(struct hashtable*));
		cache_hash_files = malloc((how_many_files)*sizeof(struct hashtable*));
		
		BSP_chunk = (chunk_t*)malloc((how_many_files)*sizeof(chunk_t));
		if (BSP_chunk == NULL){
			EXIT_TRACE("Error allocating memory for input buffer.\n");
		}

	}else{ 
		if (!S_ISREG(filestat.st_mode))
			EXIT_TRACE("not a normal file: %s\n", conf->infile);
				
		conf->file = TRUE;
		conf->folder = FALSE;
		
		stats.total_input = filestat.st_size;
				
		if((fd = open(conf->infile, O_RDONLY | O_LARGEFILE)) < 0) 
			EXIT_TRACE("%s file open error %s\n", conf->infile, strerror(errno));
	
		//If file, load entire file into memory if requested by user
		if(conf->preloading) {
		size_t bytes_read=0;
		int r;

		preloading_buffer = malloc(filestat.st_size);
		if(preloading_buffer == NULL)
			EXIT_TRACE("Error allocating memory for input buffer.\n");

		//Read data until buffer full
		while(bytes_read < filestat.st_size) {
			r = read(fd, preloading_buffer+bytes_read, filestat.st_size-bytes_read);
			if(r<0) switch(errno) {
			case EAGAIN:
				EXIT_TRACE("I/O error: No data available\n");break;
			case EBADF:
				EXIT_TRACE("I/O error: Invalid file descriptor\n");break;
			case EFAULT:
				EXIT_TRACE("I/O error: Buffer out of range\n");break;
			case EINTR:
				EXIT_TRACE("I/O error: Interruption\n");break;
			case EINVAL:
				EXIT_TRACE("I/O error: Unable to read from file descriptor\n");break;
			case EIO:
				EXIT_TRACE("I/O error: Generic I/O error\n");break;
			case EISDIR:
				EXIT_TRACE("I/O error: Cannot read from a directory\n");break;
			default:
				EXIT_TRACE("I/O error: Unrecognized error\n");break;
			}
			if(r==0) break;
			bytes_read += r;
		}

	#ifdef ENABLE_PTHREADS
		data_process_args.input_file.size = filestat.st_size;
		data_process_args.input_file.buffer = preloading_buffer;
	#else
		generic_args.input_file.size = filestat.st_size;
		generic_args.input_file.buffer = preloading_buffer;
	#endif //ENABLE_PTHREADS
		}//End if preloading #BenSP Comment*/

		//BenSP
		data_process_args.fd = fd;

	}//end IF file 
#ifdef ENABLE_PTHREADS
	/* Variables for 3 thread pools and 2 pipeline stage threads.
	 * The first and the last stage are serial (mostly I/O).
	 */
	pthread_t threads_anchor[MAX_THREADS], threads_chunk[MAX_THREADS], threads_compress[MAX_THREADS], threads_send, threads_process;

	data_process_args.tid = 0;
	data_process_args.nqueues = nqueues;

	gettimeofday(&get_init_time,NULL);
	init_time = (double)get_init_time.tv_sec + (double)get_init_time.tv_usec/1e6;

	//thread for first pipeline stage (input)
	pthread_create(&threads_process, NULL, Fragment, &data_process_args);

	//Create 3 thread pools for the intermediate pipeline stages
	struct thread_args anchor_thread_args[conf->nthreads];
	for (i = 0; i < conf->nthreads; i ++) {
	 anchor_thread_args[i].tid = i;
	 pthread_create(&threads_anchor[i], NULL, FragmentRefine, &anchor_thread_args[i]);
	}

	struct thread_args chunk_thread_args[conf->nthreads];
	for (i = 0; i < conf->nthreads; i ++) {
	chunk_thread_args[i].tid = i;
	pthread_create(&threads_chunk[i], NULL, Deduplicate, &chunk_thread_args[i]);
	}

	struct thread_args compress_thread_args[conf->nthreads];
	for (i = 0; i < conf->nthreads; i ++) {
	compress_thread_args[i].tid = i;
	pthread_create(&threads_compress[i], NULL, Compress, &compress_thread_args[i]);
	}

	//thread for last pipeline stage (output)
	struct thread_args send_block_args;
	send_block_args.tid = 0;
	send_block_args.nqueues = nqueues;
	pthread_create(&threads_send, NULL, Reorder, &send_block_args);

	/*** parallel phase ***/

	//Return values of threads
	stats_t *threads_anchor_rv[conf->nthreads];
	stats_t *threads_chunk_rv[conf->nthreads];
	stats_t *threads_compress_rv[conf->nthreads];

	//join all threads 
	pthread_join(threads_process, NULL);
	for (i = 0; i < conf->nthreads; i ++)
	pthread_join(threads_anchor[i], (void **)&threads_anchor_rv[i]);
	for (i = 0; i < conf->nthreads; i ++)
	pthread_join(threads_chunk[i], (void **)&threads_chunk_rv[i]);
	for (i = 0; i < conf->nthreads; i ++)
	pthread_join(threads_compress[i], (void **)&threads_compress_rv[i]);
	pthread_join(threads_send, NULL);


	/* free queues */
	for(i=0; i<nqueues; i++) {
		queue_destroy(&deduplicate_que[i]);
		queue_destroy(&refine_que[i]);
		queue_destroy(&reorder_que[i]);
		queue_destroy(&compress_que[i]);
	}
	free(deduplicate_que);
	free(refine_que);
	free(reorder_que);
	free(compress_que);

	//Merge everything into global `stats' structure
	for(i=0; i<conf->nthreads; i++) {
		merge_stats(&stats, threads_anchor_rv[i]);
		free(threads_anchor_rv[i]);
	}
	for(i=0; i<conf->nthreads; i++) {
		merge_stats(&stats, threads_chunk_rv[i]);
		free(threads_chunk_rv[i]);
	}
	for(i=0; i<conf->nthreads; i++) {
		merge_stats(&stats, threads_compress_rv[i]);
		free(threads_compress_rv[i]);
	}

#else //serial version

	generic_args.tid = 0;
	generic_args.nqueues = -1;
	generic_args.fd = fd;

#endif //ENABLE_PTHREADS
	gettimeofday(&get_end_time,NULL);
	end_time = (double)get_end_time.tv_sec + (double)get_end_time.tv_usec/1e6;
	//printf("---Parallel Processing OK!!\n\n");  
	
	//clean up after preloading
	if(conf->preloading) {
		free(preloading_buffer);
	}

	if (conf->file){
		/* clean up with the src file */
		if (conf->infile != NULL)
		close(fd);
	}

	assert(!mbuffer_system_destroy());

	for (int i = 0; i < total_files; i++){
		hashtable_destroy(cache_hash_files[i],TRUE);
	}
	
	//Analyze and print statistics
	if(conf->verbose){
		print_stats(&stats);	
	}else{
		BSPtime = end_time - init_time;
		printf("\tTime Processing:\t%lf seconds\n", BSPtime);	
	}
}