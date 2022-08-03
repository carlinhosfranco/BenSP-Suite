TARGET = ferret
ARCH = bensp_ferret
version = pthreads
LDLIBS += $(LIBS) -lrt -lm -lcass -lgsl -lgslcblas -ljpeg -lstdc++ -lupl
trace = notrace

PORTABILITY_FLAGS= -static-libgcc -Wl,--hash-style=both,--as-needed 

UPL = $(ROOT)/upl/include/upl
LUPL = $(ROOT)/upl/lib

#CFLAGS += -O3 -g -funroll-loops -fprefetch-loop-arrays ${PORTABILITY_FLAGS} -DPARSEC_VERSION=3.0-beta-20150206 -I$(INCDIR) -I$(UPL) -L$(LUPL)
#CXXFLAGS += -O3 -g -funroll-loops -fprefetch-loop-arrays -fpermissive -fno-exceptions ${PORTABILITY_FLAGS} -DPARSEC_VERSION=3.0-beta-20150206

#Alteração 05/05
#CFLAGS += -Os -g -funroll-loops -fprefetch-loop-arrays ${PORTABILITY_FLAGS} -I$(INCDIR) -I$(UPL) -L$(LUPL)
#CXXFLAGS += -Os -g -funroll-loops -fprefetch-loop-arrays -fpermissive -fno-exceptions ${PORTABILITY_FLAGS} 

CFLAGS += -Os -g -funroll-loops ${PORTABILITY_FLAGS} -I$(INCDIR) -I$(UPL) -L$(LUPL)
CXXFLAGS += -Os -g -funroll-loops -fpermissive -fno-exceptions ${PORTABILITY_FLAGS} 

CC = gcc
CXX = g++

#.PHONY:	all clean install dirs lib tools image server benchmark
.PHONY:	all clean install dirs lib image benchmark

#ROOT = $(CURDIR)
FERRET_DIR=$(BENSP_DIR)/apps/ferret
ROOT = ${BENSP_DIR}/apps/ferret/src
SRCDIR = $(ROOT)/src
INCDIR = $(ROOT)/include

BINDIR = $(ROOT)/$(ARCH)/bin
LIBDIR = $(ROOT)/$(ARCH)/lib
OBJDIR = $(ROOT)/$(ARCH)/obj

PREFIX = $(BINDIR)

CFLAGS += -I$(INCDIR) 
LDFLAGS += -L$(LIBDIR)


#VPATH = $(CURDIR)/src:$(CURDIR)/src/lsh:$(CURDIR)/tools:$(CURDIR)/image:$(CURDIR)/benchmark
#VPATH = $(ROOT)/src:$(ROOT)/src/lsh:$(ROOT)/tools:$(ROOT)/image:$(ROOT)/benchmark
VPATH = $(ROOT)/src:$(ROOT)/src/lsh:$(ROOT)/image:$(ROOT)/benchmark
#export LIBDIR BINDIR OBJDIR CC CXX CFLAGS LDFLAGS LDLIBS

#all:	dirs lib tools benchmark copy clean
all:	dirs lib benchmark copy clean

trace: dirs lib benchmark_trace copy clean

dirs:
	mkdir -p $(BINDIR)
	mkdir -p $(LIBDIR)
	mkdir -p $(OBJDIR)

# build the cass library
lib:	$(LIBDIR)/libcass.a 

lib_src := $(wildcard $(ROOT)/src/*.c) $(wildcard $(ROOT)/src/lsh/*.c)
lib_src := $(notdir $(lib_src))
lib_obj := $(addprefix $(OBJDIR)/, $(lib_src:.c=.o))


$(LIBDIR)/libcass.a:	$(lib_obj)
	@echo "   A  '$@'"
	@$(AR) rcs $@ $^

# build the image support library
libimage_src := image.c extract.c edge.c srm.c
libimage_obj := $(addprefix $(OBJDIR)/, $(libimage_src:.c=.o))


$(LIBDIR)/libcassimage.a:	$(libimage_obj)
	@echo "   A  '$@'"
	@$(AR) rcs $@ $^

ifdef version
  ifeq "$(version)" "pthreads"
    benchmark_base := ferret-pthreads
    LDLIBS += -lpthread
  endif
  ifeq "$(version)" "tbb"
    benchmark_base := ferret-tbb
    LDLIBS += -ltbb -ltbbmalloc
  endif
endif

ifdef trace
	ifeq "$(trace)" "trace"
		CFLAGS += -DENABLE_TRACING
	endif
endif

benchmark_src := $(benchmark_base).c


benchmark_tgt := $(notdir $(benchmark_src))
benchmark_tgt := $(addprefix $(BINDIR)/, $(benchmark_tgt:.c=))

benchmark : $(benchmark_tgt)

image_tgt := cass_img_extract
image_tgt := $(addprefix $(BINDIR)/, $(image_tgt))

image: $(image_tgt)

clean:
	-rm $(OBJDIR)/*

install:
	#cp -r $(BINDIR) $(PREFIX)
	#cp -r $(LIBDIR) $(PREFIX)
	#cp $(PREFIX)/$(benchmark_base) $(ROOT)
	#cp $(BINDIR)/$(benchmark_base) $(ROOT)
copy:
	mv $(PREFIX)/$(benchmark_base) $(FERRET_DIR)/bin/


$(OBJDIR)/%.o : %.c
	@echo "   CC '$<' ==> '$@'"
	@$(CC) $(CFLAGS) -c $< -o $@

$(OBJDIR)/%.o : %.cpp
	@echo "   CXX '$<' ==> '$@'"
	@$(CXX) $(CFLAGS) $(CXXFLAGS) -c $< -o $@

$(benchmark_tgt): $(BINDIR)/% : $(OBJDIR)/%.o $(LIBDIR)/libcass.a $(LIBDIR)/libcassimage.a
	@echo "   CXX '$<' ==> '$@'"
	@$(CXX) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

$(image_tgt): $(BINDIR)/% : $(OBJDIR)/%.o $(LIBDIR)/libcass.a $(LIBDIR)/libcassimage.a
	@echo "   CXX '$<' ==> '$@'"
	@$(CXX) $(CFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)
