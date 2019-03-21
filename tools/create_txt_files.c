/*
* *****************************************************************************
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* any later version.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, see <https://www.gnu.org/licenses/>. 
*
* The GNU General Public License does not permit incorporating your program
* into proprietary programs.  If your program is a subroutine library, you
* may consider it more useful to permit linking proprietary applications with
* the library.  If this is what you want to do, use the GNU Lesser General
* Public License instead of this License.  But first, please read
* <https://www.gnu.org/licenses/why-not-lgpl.html>.
*
* *****************************************************************************
*/

#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <getopt.h>

FILE *BSP_file;

//21 bytes size
char buf_a[] = "aaaaaaaaaaaaaaaaaaaa";
char buf_b[] = "bbbbbbbbbbbbbbbbbbbb";
char buf_c[] = "cccccccccccccccccccc";
char buf_f[] = "ffffffffffffffffffff";

int MAX_SINGLE_FILE = 0;
int HMFILES = 0;

char format_string[255];

char *randstring(size_t length);
static void usage(char* prog);

//21 bytes / size em bytes
//#define FULLSIZE	1664410 //100MB
//#define FULLSIZE 832205 //50MB
//#define FULLSIZE 499322 //30MB
#define FULLSIZE 175555
void many_files(int HOW_MANY){

	FILE *BSP_many_files[HOW_MANY];
	
	//for (int i = 0; i < HOW_MANY; i++){
	for (int i = HOW_MANY; i < HOW_MANY/2; i--){
		
		char filename[255];		
		sprintf(filename, "m_file_%d.txt",i);
		BSP_many_files[i] = fopen(filename,"w");

	}

	//for (int i = 0; i < HOW_MANY; i++){
	for (int i = HOW_MANY; i < HOW_MANY/2; i--){
		for (int j = 0; j < FULLSIZE; j++){
			#ifdef RAND
				fprintf(BSP_many_files[i], "%s\n", randstring(60));
			#elif AAR
				fprintf(BSP_many_files[i], "%s%s%s\n", buf_a,buf_a,randstring(20));
			#elif ABC
				fprintf(BSP_many_files[i], "%s%s%s\n", buf_a,buf_b,buf_c);
			#elif FFF
				fprintf(BSP_many_files[i], "%s%s%s\n", buf_f,buf_f,buf_f);
			#elif FFR
				fprintf(BSP_many_files[i], "%s%s%s\n", buf_f,buf_f,randstring(20));
			#elif RFF
				fprintf(BSP_many_files[i], "%s%s%s\n", randstring(20),buf_f,buf_f);
			#endif
		}
	}
	
	//for (int i = 0; i < HOW_MANY; i++){
	for (int i = HOW_MANY; i < HOW_MANY/2; i--){
		fclose(BSP_many_files[i]);
	}	

}

void single_file(int idx){

	FILE *BSP_single_file;
	char filename[255];
	sprintf(filename, "single_file_%d.txt",idx);
	BSP_single_file = fopen(filename, "w");

	for (int i = 0; i < FULLSIZE; i++){
		#ifdef RAND
			fprintf(BSP_single_file, "%s\n",randstring(60));
		#elif AAR
			fprintf(BSP_single_file, "%s%s%s\n", buf_a,buf_a,randstring(20));
		#elif ABC
			fprintf(BSP_single_file, "%s%s%s\n", buf_a,buf_b,buf_c);
		#elif FFF
			fprintf(BSP_single_file, "%s%s%s\n", buf_f,buf_f,buf_f);
		#elif FFR
			fprintf(BSP_single_file, "%s%s%s\n", buf_f,buf_f,randstring(20));
		#elif RFF
			fprintf(BSP_single_file, "%s%s%s\n", randstring(20),buf_f,buf_f);
		#endif
	}

	fclose(BSP_single_file);
}
char *randstring(size_t length) {

    static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!@$%¨&*()-+={}^~;:/?|";        
    char *randomString = NULL;

    if (length) {
        randomString = malloc(sizeof(char) * (length +1));

        if (randomString) {            
            for (int n = 0;n < length;n++) {            
                int key = rand() % (int)(sizeof(charset) -1);
                randomString[n] = charset[key];
            }

            randomString[length] = '\0';
        }
    }

    return randomString;
}

static void parse_options(int argc, char **argv) {

  int option_idx = 0;
  int opt;
  
  while((opt = getopt_long(argc, argv, "s:m:h", NULL, &option_idx)) != EOF){
	switch (opt){
		case 's':
			MAX_SINGLE_FILE = atoi(optarg);
			printf("%d arquivos únicos\n", MAX_SINGLE_FILE);
			break;
		case 'm':
			HMFILES = atoi(optarg);
			printf("%d múltiplos arquivos \n", HMFILES);
		  break;
		case 'h':
			usage(argv[0]);
			break;
		default:
			usage(argv[0]);
			break;
	}
  }
}


static void usage(char* prog){
  printf("usage: %s -s [NUMBERS_OF_SINGLE_FILES] -m [NUMBERS_OF_MULTIPLE_FILES]\n",prog);
  printf("usage: gcc %s -D[OPTIONS] -o a.out \n",prog);
  printf("\nSTRINGS OPTIONS:\n");
  printf("\tRAND\n");
  printf("\tAAR\n");
  printf("\tABC\n");
  printf("\tFFF\n");
  printf("\tFFR\n");
  printf("\tRFF\n");
}

int main(int argc, char *argv[]){
	
	srand(time(NULL));

	parse_options(argc,argv);
	if (HMFILES != 0){
		many_files(HMFILES);
	}else if (MAX_SINGLE_FILE != 0){
		for (int i = 0; i < MAX_SINGLE_FILE; i++){
			single_file(i);	
		}
	}else if (MAX_SINGLE_FILE == 0 && HMFILES == 0){
		printf("ERROR\n");
		usage(argv[0]);
	}
		
	return 0;
}