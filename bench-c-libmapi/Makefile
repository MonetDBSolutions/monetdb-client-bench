
CC=gcc

PKGS=monetdb-mapi
TARGET=runner
OBJS=main.o showinfo.o execute.o bench.o monetdb.o output.o

CFLAGS=-std=c99 -g -O2 -Wall -Werror $(shell pkg-config --cflags $(PKGS))
LDFLAGS=$(shell pkg-config --libs $(PKGS)) -lpthread

default: $(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

runner: $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c runner.h
	$(CC) -c -o $@ $(CFLAGS) $<

