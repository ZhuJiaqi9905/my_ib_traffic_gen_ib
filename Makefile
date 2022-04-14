.PHONY: all clean

CC = gcc
LDLIBS = -libverbs
CFLAGS = -Wall -g
APPS = ib_write_client ib_write_server ib_send_client ib_send_server
OBJS = write_client.o write_server.o send_client.o send_server.o common.o
DEPS = common.h

all: ${APPS}

ib_write_client: write_client.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

ib_write_server: write_server.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

ib_send_client: send_client.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

ib_send_server: send_server.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f *.o $(APPS)
