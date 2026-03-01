CC = gcc
CFLAGS = -std=c11 -O2 -Wall

all: c_client

c_client: client.c
	$(CC) $(CFLAGS) -o c_client client.c

clean:
	rm -f c_client
