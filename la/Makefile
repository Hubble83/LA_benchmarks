CC        = icc
CFLAGS    = -g -O3 -Wall -Wextra -std=c99 $(INCLUDE) -Wunused-variable
OPENMP    = -qopenmp
INCLUDE   = `pkg-config --cflags glib-2.0`
LIB	  	  = `pkg-config --libs glib-2.0`
VECREPORT = -qopt-report=5 -qopt-report-phase=vec

#VERBOSE   = -DD_VERBOSE
#DEBUGGING = -DD_DEBUGGING
#PROFILE   = -DD_PROFILE

OBJECTS	   = build/operators.o build/tools.o build/strings.o build/avl.o build/altavl.o

.PHONY = all clean
.DEFAULT_GOAL = all

# OBJECTS
build/strings.o: src/common/strings.c
	$(CC) -c $(CFLAGS) -o $@ $<

build/tools.o: src/common/tools.c
	$(CC) -c $(CFLAGS) -o $@ $< $(LIB)

build/operators.o: src/common/operators.c
	$(CC) -c $(CFLAGS) $(OPENMP) $(VECREPORT) -o $@ $< $(LIB)

build/avl.o: src/common/avl.c
	$(CC) -c $(CFLAGS) -o $@ $<

build/altavl.o: src/common/altavl.c build/avl.o
	$(CC) -c $(CFLAGS) -o $@ $^

# QUERY 3
bin/query_3_seq: src/queries/query_3_seq.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

bin/query_3_par: src/queries/query_3_par.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

# QUERY 4
bin/query_4: src/queries/query_4.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

# QUERY 6
bin/query_6_seq: src/queries/query_6_seq.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@  $^ $(LIB) $(PROFILE) $(VERBOSE)

bin/query_6_par: src/queries/query_6_par.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

# QUERY 11
bin/query_11: src/queries/query_11.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

# QUERY 12
bin/query_12: src/queries/query_12.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

# QUERY 14
bin/query_14: src/queries/query_14.c $(OBJECTS)
	$(CC) $(CFLAGS) $(OPENMP) -o $@ $^ $(LIB) $(PROFILE) $(VERBOSE)

checkdirs:
	@mkdir -p bin
	@mkdir -p build

all: checkdirs\
	 $(OBJECTS)\
	 bin/query_3_seq\
	 bin/query_3_par\
	 bin/query_4\
	 bin/query_6_seq\
	 bin/query_6_par\
	 bin/query_11\
	 bin/query_12\
	 bin/query_14

clean:
	rm -fr bin build
