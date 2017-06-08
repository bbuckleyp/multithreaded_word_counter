#include "pj3_datastruct.h"

int REPLICAS;          // contains the number of threads passed in through **argv
int readerDone;        // global flag indicating if the reader threads have completed

int main( int argc, char *argv[] ) {
	int bufSize, i, rc;
	long fileSize, start, end;
	char *filename;
	FILE *fd;
	void *dummy;
	pthread_t mreader_threads[ atoi(argv[2]) ];
	pthread_t madder_threads[ atoi(argv[2]) ];
	pthread_mutex_t mutexArray[ atoi(argv[2]) ];
	pthread_t reducer_thread;
	pthread_attr_t attr1;
	pthread_attr_t attr2;
	pthread_attr_t attr3;
	struct buffer bufferArray[ atoi(argv[2]) ];
	struct rdData rdDataArray[ atoi(argv[2]) ];
	struct varBuf varBufArray[ atoi(argv[2]) ];
	struct bufbuf bufbufArray[ atoi(argv[2]) ];

	// verifying that 3 arguments were passed in
	if( argc != 4 ) {
		printf( "%s\n", "Error, needs 3 arguements" );
		return -1;
	} else {
		// importing all the variables
		filename = argv[1];
		REPLICAS = atoi( argv[2] );
 		bufSize = atoi( argv[3] );

		// printing variables to stdout
		printf( "\n\nFilename: %s\n", filename );
		printf( "Replicas: %d\n", REPLICAS );
		printf( "Buf Size: %d\n\n", bufSize );
		
		// getting the size of the file
		fd = fopen( filename, "r" );
		fseek( fd, 0, SEEK_END ); // seeking to the end
		fileSize = ftell( fd );   // saving size
		fseek( fd, 0, SEEK_SET ); // seeking back to the beginning

		printf( "file size = %ld\n", fileSize );

		// initialize all data structures
		for( i = 0; i < REPLICAS; i++ ) {
			pthread_mutex_init( &mutexArray[i], NULL );
			bufferInit( &bufferArray[i], bufSize, mutexArray[i] );
			start = returnStart( fd, fileSize, (i + 1) );
			end = returnEnd( fd, fileSize, (i + 1) );
			rdDataInit( &rdDataArray[i], filename, start , end, &bufferArray[i] );
			varBufInit( &varBufArray[i] );
			bufbufInit( &bufbufArray[i], &bufferArray[i], &varBufArray[i] );
		}

		// closing file
		fclose( fd );

		// Initializing mapper threads and set thread detcached attribute
		pthread_attr_init( &attr1 );
		pthread_attr_setdetachstate( &attr1, PTHREAD_CREATE_JOINABLE );
		pthread_attr_init( &attr2 );
		pthread_attr_setdetachstate( &attr2, PTHREAD_CREATE_JOINABLE );

		for( i = 0; i < REPLICAS; i++ ) {
			printf("Main: creating mapper threads %d\n", i);
			rc = pthread_create( &mreader_threads[i], &attr1, &map_reader, (void*)&rdDataArray[i] );
			if( rc ) {
				printf( "Error; return code from pthread_create( READER ) is %d\n", rc );
				exit(-1);
			}
			rc = pthread_create( &madder_threads[i], &attr2, &map_adder, (void*)&bufbufArray[i] );
			if( rc ) {
				printf( "Error; return code from pthread_create( ADDER ) is %d\n", rc );
				exit(-1);
			}
		}

		// Free attribute and wait for the other threads
		pthread_attr_destroy( &attr1 );
		
		for( i = 0; i < REPLICAS; i++ ) {
			rc = pthread_join( mreader_threads[i], &dummy );
			if( rc ) {
				printf( "Error; return code from pthread_join( READER ) is %d\n", rc );
				exit(-1);
			}
			printf("Main: completed join with READER thread %d\n", i);
		}

		// updating flag to indicate all reader threads have exited
		readerDone = 1;
		pthread_attr_destroy( &attr2 );

		for( i = 0; i < REPLICAS; i++ ) {
			rc = pthread_join( madder_threads[i], &dummy );
			if( rc ) {
				printf( "Error; return code from pthread_join( ADDER ) is %d\n", rc );
				exit(-1);
			}
			printf("Main: completed join with ADDER thread %d\n", i);
		}
		
		// Initializing reducer thread and set thread detcached attribute
		pthread_attr_init( &attr3 );
		pthread_attr_setdetachstate( &attr3, PTHREAD_CREATE_JOINABLE );

		printf("Main: creating reducer thread\n");

		rc = pthread_create( &reducer_thread, &attr3, &reducer, (void*)bufbufArray );
		if( rc ) {
			printf( "Error; return code from pthread_create( REDUCER ) is %d\n", rc );
			exit(-1);
		}

		// Free attribute and wait for thread
		pthread_attr_destroy( &attr3 );

		rc = pthread_join( reducer_thread, &dummy );
		if( rc ) {
			printf( "Error; return code from pthread_join( REDUCER ) is %d\n", rc );
			exit(-1);
		}
		printf("Main: completed join with REDUCER thread %d\n", i);

		// Freeing all malloced data
		for( i = 0; i < REPLICAS; i++ ) { 
			free( bufbufArray[i].buf1->dir );
			free( bufbufArray[i].buf2->dir );
			pthread_mutex_destroy( &mutexArray[i] );
		}

		printf( "Main: program completed. Exiting.\n" );
		pthread_exit( NULL );
	}
}

// Description: calculating the starting postion.
//              increments to the next place until it finds whitespace
// Return: 		file poistion if found
// 				-1 if EOF reached
long returnStart( FILE *file, long fileSize, int bufNum ) {
	long currPos =  ( (bufNum - 1) * fileSize ) / REPLICAS ;
	int c; 

	fseek( file, currPos, SEEK_SET );

	if( currPos == 0 ) {
		return( currPos );
	}

	// finding the next position that is whitespace or \n
	while( (c = fgetc(file)) != EOF ) {
		if( isspace(c) != 0 ) {
			return ( ftell( file ) );
		}
	}
	return -1;
}

// Description: calculating the end position
//              increments to the next place until it finds whitespace
// Return: 		file poistion
long returnEnd( FILE *file, long fileSize, int bufNum ) {
	long mvPos =  fileSize  / REPLICAS;
	int c; 

	fseek( file, mvPos, SEEK_CUR );

	// finding the next position that is whitespace or \n
	while( (c = fgetc(file)) != EOF ) {
		if( isspace(c) != 0 ) {
			mvPos = ftell( file );
			if( mvPos > fileSize ) {
				mvPos = fileSize;
			}
			return( mvPos );
		}
	}
	mvPos = ftell( file );
	if( mvPos > fileSize ) {
		mvPos = fileSize;
	}
	return( mvPos );
}


// Description: initialing buffer
void bufferInit( struct buffer *buf, int size, pthread_mutex_t mutex ) {
	buf->mutex =  mutex;
	buf->count = 0;
	buf->maxSize = size;
	buf->dir = (pair*)malloc( sizeof(pair)*size );
	printf( "reader buffer initialized\n" );
}

// Description: initialing varBuf
void varBufInit( struct varBuf *buf ) {
	buf->size = INC_BUFF_SIZE;
	buf->count = 0;
	buf->dir = (pair*)malloc( sizeof(pair) * INC_BUFF_SIZE );
	printf( "adder buffer initialized\n" );
}

// Description: initializing rdData
void rdDataInit( struct rdData *data, char *filename, long start, long end, struct buffer *buf ) {
	data->filename = filename;
	data->start = start;
	data->end = end;
	data->buf = buf;
	printf( "read structure initialized\n" );
}

// Description: initialzing bufbuf
void bufbufInit( struct bufbuf *buffers, struct buffer *buf, struct varBuf *vbuf ) {
	buffers->buf1 = buf;
	buffers->buf2 = vbuf;
	printf( "buffer container initialized\n" );
}

// Description: adds a pair to the buffer
// Return: 0 if success
// 		  -1 if failure
int addEntry( struct buffer *b, struct pair *p ) {
	// locking critical section
	pthread_mutex_lock( &b->mutex );
	if( b->count < b->maxSize ) {
		b->dir[b->count] = *p;
		b->count++;
		pthread_mutex_unlock( &b->mutex );
		return 0;
	}
	// unlock critical section
	pthread_mutex_unlock( &b->mutex );
	return -1;
}

// Description: grabs a pair from the buffer
// Return: returns a valid pair if success
// 		  -1 if failure
int  grabEntry( struct buffer *b, struct pair *grabpair ) {
	if( b->count > 0 ) {
		b->count--;
		*grabpair = b->dir[b->count];
		return 0;
	}
	return -1;
}

void *map_reader( void *rdDat ) {
	// Declaring/Initializing Variables
	struct rdData *data = (rdData*)rdDat;
	struct pair newPair;
	FILE *file = fopen( data->filename, "r" );

	// checking if file open was successfull
	if( file == NULL ) {
		printf( "Could not open the file\n" );
		pthread_exit( NULL );
	}

	// seeking to file position
	if( fseek( file, data->start, SEEK_SET ) != 0 ) {
		printf( "Seek failed\n" );
		fclose( file );
		pthread_exit( NULL );
	}

	// grabbing a word and adding to buffer
	while( ftell(file) < data->end ) {
		fscanf( file, "%s", newPair.name );
		newPair.count = 1;
		while( addEntry(data->buf, &newPair) == -1 );
	}

	// removing last entry
	pthread_mutex_lock( &data->buf->mutex );
	data->buf->count--;
	data->buf->dir[data->buf->count].name[0] = (char)'\0';
	data->buf->dir[data->buf->count].count = 0;
	pthread_mutex_unlock( &data->buf->mutex );

	fclose( file );
	pthread_exit( NULL );
}

void *map_adder( void *buffers ) {
	// Declaring/Initializing Variables
	struct bufbuf *buf = (bufbuf*)buffers;
	struct pair mypair = {{'\0'}, -1};
	int i = 0;
	struct varBuf *newBuf;
	int isFound = 0;

	// running while the reader threads are still running and there are still contents in the buffer
	while( readerDone == 0 ) { 
		// extrating a pair from buffer
		mypair.count = -1;

		pthread_mutex_lock( &buf->buf1->mutex );
		if( buf->buf1->count > 0 ) {
			while( grabEntry( buf->buf1, &mypair ) == -1 );
		}
		pthread_mutex_unlock( &buf->buf1->mutex );

		// searching through varBuf to update an entry or add an entry
		if( mypair.count > 0 ) {
			isFound = 0;
			i = 0;
			while( i < buf->buf2->count && isFound == 0 ) {
				// checking if words match
				if( !strcmp( mypair.name, buf->buf2->dir[i].name ) ) { 
					buf->buf2->dir[i].count++;
					isFound = 1;
				}
				i++;
			}

			// checking if not found
			if( isFound == 0 ) {
				// checking if buffer needs expanded
				if( buf->buf2->count >= buf->buf2->size ) {
					newBuf = (varBuf*)malloc( sizeof( varBuf ) );
					// make new buffer of larger size
					newBuf->dir = (pair*)malloc( (sizeof(pair) * buf->buf2->size) + (sizeof(pair) * INC_BUFF_SIZE) );
					// copying data over
					memcpy( newBuf->dir, buf->buf2->dir, (sizeof(pair) * buf->buf2->size) ); 
					// updating variables
					newBuf->size = buf->buf2->size + INC_BUFF_SIZE; newBuf->count = buf->buf2->count;
					newBuf->count = buf->buf2->count;
					// freeing old buffer and updating
					free(buf->buf2->dir);
					buf->buf2 = newBuf;
				}
				// adding entry to end of dir in varBuf
				memcpy( buf->buf2->dir[i].name, mypair.name, sizeof( mypair.name ) );
				buf->buf2->dir[i].count = 1;
				buf->buf2->count++;
			}
		}
	}

	while( buf->buf1->count > 0 ) {
		// extrating a pair from buffer
		mypair.count = -1;
		while( grabEntry( buf->buf1, &mypair ) == -1 );
		pthread_mutex_unlock( &buf->buf1->mutex );  

		if( mypair.count > 0 ) {
			// searching through varBuf to update an entry or add an entry
			isFound = 0;
			i = 0;
			while( i < buf->buf2->count && isFound == 0 ) {
				// checking if words match
				if( !strcmp( mypair.name, buf->buf2->dir[i].name ) ) { 
					buf->buf2->dir[i].count++;
					isFound = 1;
				}
				i++;
			}

			// checking if not found
			if( isFound == 0 ) {
				// checking if buffer needs expanded
				if( buf->buf2->count >= buf->buf2->size ) {
					newBuf = (varBuf*)malloc( sizeof( varBuf ) );
					// make new buffer of larger size
					newBuf->dir = (pair*)malloc( (sizeof(pair) * buf->buf2->size) + (sizeof(pair) * INC_BUFF_SIZE) );
					// copying data over
					memcpy( newBuf->dir, buf->buf2->dir, (sizeof(pair) * buf->buf2->size) ); 
					// updating variables
					newBuf->size = buf->buf2->size + INC_BUFF_SIZE;
					newBuf->count = buf->buf2->count;
					// freeing old buffer and updating
					free(buf->buf2->dir);
					buf->buf2 = newBuf;
				}
				// adding entry to end of dir in varBuf
				memcpy( buf->buf2->dir[i].name, mypair.name, sizeof( mypair.name ) );
				buf->buf2->dir[i].count = 1;
				buf->buf2->count++;
			}
		}
	}
	pthread_exit( NULL );
}

void *reducer( void *bufarray ) { 
	struct bufbuf *buflist = (bufbuf*)bufarray;
	FILE *file = fopen( "./output.txt", "w" );
	int currCount = 0, i, j, k, l;
	char currWord[16];

	// search through varBufs in buflist and output to file
	for( i = 0; i < REPLICAS; i++ ) {
		// extracting a word out of the buffer
		for( j = 0; j < buflist[i].buf2->count; j++ ) {
			// checking if the count has been cleared (if cleared, it has already been accounted for)
			if( buflist[i].buf2->dir[j].count != 0 ) {
				strcpy( currWord, buflist[i].buf2->dir[j].name );  // sizeof(currWord) );
				currCount = buflist[i].buf2->dir[j].count;

				// searching the rest of the buffers
				for( k = (i + 1); k < REPLICAS; k++ ) { 
					// searching the remaining words in the buffer
					for( l = 0; l < buflist[k].buf2->count; l++ ) {
						// checking if the words match and updating variables
						if( !strcmp( currWord, buflist[k].buf2->dir[l].name ) ) { 
							currCount += buflist[k].buf2->dir[l].count;
							buflist[k].buf2->dir[l].count = 0;
						}
					}
				}

				fprintf( file, "%d\t\t", currCount );
				fprintf( file, "%.16s\n", currWord );
			}
		}
	}
	fclose( file );
	pthread_exit( NULL );
}
