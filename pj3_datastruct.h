#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <ctype.h>

#define INC_BUFF_SIZE 20


// data structures 
typedef struct bufbuf {
	struct buffer *buf1;
	struct varBuf *buf2;
} bufbuf;

typedef struct buffer {
	pthread_mutex_t mutex;
	int   count;   // number of pairs in buffer
	int   maxSize; // maximum size of the buffer
	struct pair *dir;     // directory of pairs
} buffer;

typedef struct varBuf {
	int	  size;    // size of buffer
	int   count;   // number of entries
	struct pair *dir;     // directory of pairs
} varBuf;

typedef struct rdData {
	char   *filename; // name of the file
	long    start;    // start of file
	long    end;	  // end of file
	struct buffer *buf;      // pointer to the buffer
} rdData;

typedef struct pair {
	char name[16]; // word name
	int  count;	   // number of times name exists
} pair;



// function prototypes
long returnStart( FILE *file, long fileSize, int bufNum );
long returnEnd( FILE *file, long fileSize, int bufNum );
void bufferInit( struct buffer *buf, int size, pthread_mutex_t mutex );
void varBufInit( struct varBuf *buf );
void rdDataInit( struct rdData *data, char *filename, long start, long end, struct buffer *buf );
void bufbufInit( struct bufbuf *buffers, struct buffer *buf, struct varBuf *vbuf );
int addEntry( struct buffer* b, struct pair* p );
int grabEntry( struct buffer *b, struct pair *grabpair );
void *map_reader( void *rdData );
void *map_adder( void *buffers );
void *reducer( void *bufarray );
