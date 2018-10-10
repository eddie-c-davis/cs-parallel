SEQCC=gcc
PARCC=mpicc

SEQFLAGS=-Wall -g -O3
PARFLAGS=-Wall -g -O3 -fopenmp

LDFLAGS=-lm -lprand
INC=-I/usr/local/include
LINK=-L/usr/local/lib

PROGS=mympi bucketsort-seq bucketsort-par

all: $(PROGS)

mympi: mympi.o
	$(PARCC) $(PARFLAGS) -o $< $(LDFLAGS) -c

bucketsort-seq:	bucketsort-seq.o
	$(SEQCC) $(SEQFLAGS) -o $@ bucketsort-seq.o $(LDFLAGS)
	cp -f ./bucketsort-seq ./sequential-bucketsort

bucketsort-par: bucketsort-par.o
	$(PARCC) $(PARFLAGS) -o $@ $< $(LDFLAGS) mympi.o
	cp -f ./bucketsort-par ./parallel-bucketsort

clean:
	/bin/rm -f *.o *-bucketsort $(PROGS)
