# Makefile for dedup kernel
CC=/usr/bin/gcc
SPAR=$(HOME)/spar/bin/spar

DIR = ${BENSP_DIR}/apps/dedup/
DIR_SRC=${BENSP_DIR}/apps/dedup/src/

CFLAGS += -Wall -fno-strict-aliasing -D_XOPEN_SOURCE=600
CF = -Os -g -funroll-loops -static-libgcc -Wl,--hash-style=both,--as-needed

SSL = $(DIR)/libs/ssl/include
LSSL = $(DIR)/libs/ssl/lib

ZLIB =$(DIR)/libs/zlib/include
LZLIB =$(DIR)/libs/zlib/lib

CFLAGS += -DENABLE_UPL
UPL = $(DIR)/libs/upl/include/upl
LUPL = $(DIR)/libs/upl/lib

LIBS += -lm

# Uncomment the following to enable gzip compression
CFLAGS += -DENABLE_GZIP_COMPRESSION 
LIBS += -lz

# Uncomment the following to enable bzip2 compression
CFLAGS += -DENABLE_BZIP2_COMPRESSION
LIBS += -lbz2

CFLAGS += -DENABLE_PTHREADS -pthread

CFLAGS_SPAR = -DENABLE_UPL -DENABLE_SPAR -pthread -fpermissive
SPAR_FLAGS = -spar_blocking -spar_ondemand

TRACE_FLAGS = -DENABLE_TRACING

spar:
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) hashtable.c -o hashtable.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) util.c -o util.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) dedup.c -o dedup.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) rabin.c -o rabin.o
	$(SPAR) -std=c++11 -c $(CF) $(CFLAGS) $(CFLAGS_SPAR) -I $(SSL) -I $(UPL) -I $(ZLIB) -spar_file encoder_spar.cpp -o encoder_spar.o $(SPAR_FLAGS) -spar_ordered
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(UPL) -I $(ZLIB) decoder.c -o decoder.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) mbuffer.c -o mbuffer.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) sha.c -o sha.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) queue.c -o queue.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) binheap.c -o binheap.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) tree.c -o tree.o
	g++ $(CF) $(CFLAGS) -I$(SSL) -I$(UPL) $(CFLAGS_SPAR) -I$(ZLIB) -L$(LZLIB) -L$(LUPL) -L$(LSSL) -L/usr/lib64 -L/usr/lib -o dedup_spar hashtable.o util.o dedup.o rabin.o encoder_spar.o decoder.o mbuffer.o sha.o queue.o binheap.o tree.o  -lm -lz -lupl -lbz2 
	rm -f *.o

spar-ord:
	#CFLAGS += -DENABLE_UPL -DENABLE_SPAR -pthread -fpermissive
	#SPAR_FLAGS = -spar_blocking -spar_ondemand
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) hashtable.c -o hashtable.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) util.c -o util.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) dedup.c -o dedup.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) rabin.c -o rabin.o
	$(SPAR) -std=c++11 -c $(CF) $(CFLAGS) -I $(SSL) -I $(UPL) -I $(ZLIB) -spar_file encoder_spar_ord.cpp -o encoder_spar_ord.o $(SPAR_FLAGS)
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(UPL) -I $(ZLIB) decoder.c -o decoder.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) mbuffer.c -o mbuffer.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) sha.c -o sha.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) queue.c -o queue.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) binheap.c -o binheap.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) tree.c -o tree.o
	g++ $(CF) $(CFLAGS) -I$(SSL) -I$(UPL)  -I$(ZLIB) -L$(LZLIB) -L$(LUPL) -L$(LSSL) -L/usr/lib64 -L/usr/lib -o dedup_spar_ord hashtable.o util.o dedup.o rabin.o encoder_spar_ord.o decoder.o mbuffer.o sha.o queue.o binheap.o tree.o  -lm -lz -lupl -lbz2
	rm -f *.o

bensp:
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)hashtable.c -o $(DIR_SRC)hashtable.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)util.c -o $(DIR_SRC)util.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)dedup.c -o $(DIR_SRC)dedup.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)rabin.c -o $(DIR_SRC)rabin.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) -I $(UPL)  $(DIR_SRC)new_dedup.c -o $(DIR_SRC)new_dedup.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) -I $(UPL)  $(DIR_SRC)decoder.c -o $(DIR_SRC)decoder.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)mbuffer.c -o $(DIR_SRC)mbuffer.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)sha.c -o $(DIR_SRC)sha.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)queue.c -o $(DIR_SRC)queue.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)binheap.c -o $(DIR_SRC)binheap.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)tree.c -o $(DIR_SRC)tree.o
	$(CC) $(CF) $(CFLAGS) -I$(SSL) -I$(UPL) -I$(ZLIB) -L$(LZLIB) -L$(LSSL) -L$(LUPL) -L/usr/lib64 -L/usr/lib -o $(DEDUP_DIR)/bin/dedup_pthreads $(DIR_SRC)hashtable.o $(DIR_SRC)util.o $(DIR_SRC)dedup.o $(DIR_SRC)rabin.o $(DIR_SRC)new_dedup.o $(DIR_SRC)decoder.o $(DIR_SRC)mbuffer.o $(DIR_SRC)sha.o $(DIR_SRC)queue.o $(DIR_SRC)binheap.o $(DIR_SRC)tree.o  -lm -lz -lupl -lbz2

	rm -f $(DIR_SRC)*.o

trace:
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)hashtable.c -o $(DIR_SRC)hashtable.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)util.c -o $(DIR_SRC)util.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)dedup.c -o $(DIR_SRC)dedup.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)rabin.c -o $(DIR_SRC)rabin.o
	$(CC) -c $(CF) $(CFLAGS) $(TRACE_FLAGS) -I $(SSL) -I $(ZLIB) -I $(UPL)  $(DIR_SRC)new_dedup.c -o $(DIR_SRC)new_dedup.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) -I $(UPL)  $(DIR_SRC)decoder.c -o $(DIR_SRC)decoder.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)mbuffer.c -o $(DIR_SRC)mbuffer.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)sha.c -o $(DIR_SRC)sha.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)queue.c -o $(DIR_SRC)queue.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)binheap.c -o $(DIR_SRC)binheap.o
	$(CC) -c $(CF) $(CFLAGS) -I $(SSL) -I $(ZLIB) $(DIR_SRC)tree.c -o $(DIR_SRC)tree.o
	$(CC) $(CF) $(CFLAGS) -I$(SSL) -I$(UPL) -I$(ZLIB) -L$(LZLIB) -L$(LSSL) -L$(LUPL) -L/usr/lib64 -L/usr/lib -o $(DEDUP_DIR)/bin/dedup_pthreads $(DIR_SRC)hashtable.o $(DIR_SRC)util.o $(DIR_SRC)dedup.o $(DIR_SRC)rabin.o $(DIR_SRC)new_dedup.o $(DIR_SRC)decoder.o $(DIR_SRC)mbuffer.o $(DIR_SRC)sha.o $(DIR_SRC)queue.o $(DIR_SRC)binheap.o $(DIR_SRC)tree.o  -lm -lz -lupl -lbz2

	rm -f $(DIR_SRC)*.o


clean:
	rm -f *.o
