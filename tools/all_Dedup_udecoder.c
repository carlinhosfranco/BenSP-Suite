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

#include "upl.h"

/*
Coment: gcc all_udecoder.c -o all_udecoder -I ../apps/dedup/libs/upl/include/upl/ -L ../app/dedup/libs/upl/lib/ -lupl
*/

void exec(char *in, char *out){

		
	char buff[256];
	sprintf(buff,"./dedup_pthreads -u -v -i %s -o %s",in,out);

	char *dat = UPL_getCommandResult(buff);
}

int main(int argc, char **argv[]){
	
	DIR *folder;
	struct dirent *in_file;
	struct stat file;

	size_t size_string = 0;

	char *in_folder = argv[1];
	char *out_folder = argv[2];

	char path_file_in[256];
	char path_file_out[256];
	
	if (NULL == (folder = opendir(in_folder)) ){
		printf("Erro open folder\n");
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
		
		sprintf(path_file_in,"%s/%s",in_folder,in_file->d_name);
		
		sprintf(path_file_out,"%s/%s.%s",out_folder,in_file->d_name,"tar");
		
		printf("Lendo: %s  Processando: %s \n", path_file_in,path_file_out);
		
		exec(path_file_in,path_file_out);	
	}

	return 0;
}