#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include "server.h"

//#define PORT 9009 
#define MAX_WORKERS 4 
#define BUFFER_SIZE 1024
#define MAX_SUPPORTED_CHUNK_SIZE 4096

//1-D array to store worker socket id
int client_sockets[MAX_WORKERS];

//1-D array to track which worker is alive
int is_alive[MAX_WORKERS];

//take array of mutexes to handle fault tollerence by workers. In case of fault tollerence
//different workers can read and write to same socket at a time so one mutex for each worker socket index
pthread_mutex_t w_socket_mutexes[MAX_WORKERS];

//mutex to track all the worker has finished thier work
pthread_mutex_t chunks_received_mutex = PTHREAD_MUTEX_INITIALIZER;

//2-D array to store each sorted chunk received from workers
//each worker node will sent back one sorted array, so we are storing inside 2-D to implement k-way merge.
int *received_chunks[MAX_WORKERS];

//1-D array to store chunk size used by each worker
int chunk_sizes[MAX_WORKERS];
int chunks_received = 0;
int server_socket;

// Function prototypes
void *worker_handler(void *arg);
void merge(int *data, int left, int mid, int right);
void merge_sort(int *data, int left, int right);
void merge_chunks(int num_chunks, int *chunks[], int chunk_sizes[], int total_size);
void signal_handler(int signum);

//object to pass during thread creation to each worker function, so that each worker can work on his own object
typedef struct {
    int worker_index;
    int chunk_size;
    int *chunk;
} WorkerData;

void signal_handler(int signum) {
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (client_sockets[i] != -1) {
            close(client_sockets[i]);
        }
    }
    close(server_socket);
    exit(0);
}

void read_conf_file(char *file_name)
{
        FILE *fp;
        char buffer[MAXLINE];
        char *token;

        fp = fopen(file_name, "r");
        if(!fp) {
                printf("Error in opening input conf file: %s\n", file_name);
                goto out;
        }

        memset(buffer, 0, sizeof(buffer));
        fscanf(fp, "%s", buffer);
        token = strtok(buffer, "=\n");
        if (strcmp(token, "SERVER_PORT")) {
                printf("Invalid string in client.conf file, it should be SERVER_PORT=<server-port-number>\n");
                goto out;
        }
        while(token != NULL) {
                token = strtok(NULL, "=\n");
                if (token == NULL) break;
                server_port = atoi(token);
        }
        printf("server port number is %d\n", server_port);
out:
        fclose(fp);

        return;
}


int main(int argc, char *argv[]) {
    //int server_socket;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    pthread_t threads[MAX_WORKERS];
    WorkerData worker_data[MAX_WORKERS];

    if (argc != 2) {
            printf("Please provide server.conf as input of server program\n");
            exit(1);
    }

    //to handle Ctrl-C
    signal(SIGINT, signal_handler);

    //to handle broken pipe, when server worker thread tries to write to the worker socket which is mapped to node1
    //socket and the node1 socket is closed say because of the node1 is down.
    //So in this case send/recv() calls may return SIGPIPE, signal value 13, and the server process terminates
    //with exit code 128 + 31 = 141, echo $? output. So to avoid server process termination we need to handle 
    //SIGPIPE signal inside the server code i.e. we can ignore this signal, because to check node fault we must 
    //read/write() to the worker socket which is mapped to the down node1 and when it returns error then we try
    //to send this request to some other avaialable node. in this case two things happen the read/write() call
    //will return -1 or 0 and the kernel will return SIGPIPE signal to the calling process.
    signal(SIGPIPE, SIG_IGN);

    read_conf_file(argv[1]);

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Binding failed");
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    if (listen(server_socket, MAX_WORKERS) == -1) {
        perror("Listening failed");
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    printf("Server is running and waiting for worker connections...\n");

    // Accept exactly MAX_WORKERS connections
    for (int i = 0; i < MAX_WORKERS; i++) {
        client_sockets[i] = accept(server_socket, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_sockets[i] == -1) {
            perror("Connection acceptance failed");
            close(server_socket);
            exit(EXIT_FAILURE);
        }
        is_alive[i] = 1;
        printf("Worker %d connected\n", i + 1);
    }

    // Main server loop to handle multiple files
    while (1) {
        char filename[256];
        printf("Enter the filename to sort (or 'exit' to quit): ");
        scanf("%s", filename);

        if (strcmp(filename, "exit") == 0) {
            printf("Exiting server...\n");
            break;
        }

        // Open file and count total integers
        FILE *file = fopen(filename, "r");
        if (!file) {
            perror("Error opening file");
            continue;
        }

        int total_integers = 0;
        int temp;
        while (fscanf(file, "%d", &temp) != EOF) {
            total_integers++;
        }
        rewind(file);

        // Calculate number of integers per chunk
        int integers_per_chunk = total_integers / MAX_WORKERS;
        int remainder = total_integers % MAX_WORKERS;

        // Divide data into chunks
        int *chunks[MAX_WORKERS];
        for (int i = 0; i < MAX_WORKERS; i++) {
            int chunk_size = (i < remainder) ? integers_per_chunk + 1 : integers_per_chunk;

	    if (chunk_size > MAX_SUPPORTED_CHUNK_SIZE ) {
		printf("This application doesn't supports chunk size more than 4096 bytes\n");
		exit(1);
	    }

	    /*
	     * let's malloc equal to chunk size, it could be 4 MB, if total size is 16MB and we have 4 worker nodes
	     * it could be improve by parallely reading the file and populating the input chunk for each node
	     * node0 -> [4MB sent chunk read from input file and stored in memory to sent to client in 4K/1Kpages,1000page]
	     * node1 -> [4MB read from the file]
	     * node2 -> [4MB read from the file]
	     * node3 -> [4MB read from the file]
	     */

            chunks[i] = (int *)malloc(sizeof(int) * chunk_size);
            if (chunks[i] == NULL) {
                printf("Malloc failed\n");
                exit(1);
            }
            for (int j = 0; j < chunk_size; j++) {
                fscanf(file, "%d", &chunks[i][j]);
            }
            chunk_sizes[i] = chunk_size;
        }
        fclose(file);

	//initialize all the mutexes
        for (int i = 0; i < MAX_WORKERS; i++) {
            pthread_mutex_init(&w_socket_mutexes[i], NULL);
            is_alive[i] = 1;
        }

	memset(worker_data, 0, sizeof(worker_data));
        memset(threads, 0, sizeof(threads));
        chunks_received = 0;
        pthread_mutex_init(&chunks_received_mutex, NULL);

        // Send chunks to workers and create threads for fault tolerance
        for (int i = 0; i < MAX_WORKERS; i++) {
            if (is_alive[i]) {
                worker_data[i].worker_index = i;
                worker_data[i].chunk_size = chunk_sizes[i];
                worker_data[i].chunk = chunks[i];

                /*
		 * Allocate space to store sorted chunk from this worker
		 * here again allocate memory for each chunk for each worker node received data, to do the k-way merge
		 * here it again allocates the memeory 4MB for each worker node, if total input size is 16MB and 4 worker nodes
		 * node0->[4MB sorted data received from the client it will recievd in 4K/1K page size to the client, 1000pages]
		 * node1->[4MB sorted data received]
		 * node3->[4MB sorted data received]
		 * node4->[4MB sorted data received]
		 */

                received_chunks[i] = (int *)malloc(sizeof(int) * chunk_sizes[i]);
                if (received_chunks[i] == NULL ) {
                    printf("malloc failed, exiting\n");
                    exit(1);
                }
                memset(received_chunks[i], 0, sizeof(int) * chunk_sizes[i]);

                // Create worker thread
                pthread_create(&threads[i], NULL, worker_handler, (void *)&worker_data[i]);
            }
        }

        // Wait for threads to finish
        for (int i = 0; i < MAX_WORKERS; i++) {
            pthread_join(threads[i], NULL);
        }

        // After all chunks have been received, merge them
        if (chunks_received == MAX_WORKERS) {
            merge_chunks(MAX_WORKERS, received_chunks, chunk_sizes, total_integers);
            printf("Sorting completed for file %s. Output saved to output.txt\n", filename);
        }

        // Free allocated chunks
        for (int i = 0; i < MAX_WORKERS; i++) {
            free(chunks[i]);
            chunks[i] = NULL;
            free(received_chunks[i]);
            received_chunks[i] = NULL;
            pthread_mutex_destroy(&w_socket_mutexes[i]);
            chunk_sizes[i] = 0;
            is_alive[i] = 1;
        }
       chunks_received = 0;
       pthread_mutex_destroy(&chunks_received_mutex);

    }//main server loop


    // Close sockets
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (client_sockets[i] != -1) {
            close(client_sockets[i]);
        }
    }

    close(server_socket);
    return 0;
}

void *worker_handler(void *arg) {
    WorkerData *data = (WorkerData *)arg;
    int *chunk = data->chunk;
    int chunk_size = data->chunk_size;
    int initial_worker_index = data->worker_index;
    int current_worker_index = initial_worker_index;
    int sent_bytes, received_bytes;
    const int delay_time = 100000;  // Delay time in microseconds (100ms)

	//start from offset 0 of each chunk and send whole chunk in batches if total chunk size is more than the
	//buffer capacity of the node. Say chunk size is 4K and the node is having buffer capacity 1K then send
        //4 pages each of size 1K from here and then recv the pages page wise again here.

    int offset = 0;
 
    while (1) {

        printf("Worker %d is sending chunk\n", current_worker_index + 1);
        for (int i = 0; i < chunk_size; i++) {
                printf(" %d ", chunk[i]);
        }
        printf("\n");


	/* 
	 * in reassignment during fault tollerence it is possible two or more threads can
	 * try to send or recv to the same socket, so needed mutex per socket to avoid the data corruption, think like
	 * two threads might tried to write on same socket at the same time, so needed mutex to synchronize it, each
	 * thread first need to grab the mutex and then only can write to the socket.
	 * this mutex will make sure if say node1 is having s1 --> s2 it will complete the processign of s1 complete
	 * batch only then it will give chance to another worker if they can leverage this s1 socket if because of
	 * the fault tollerence any worker is not able to send in his own socket.
         * s1 --> s3
         * s2 --> s4
	 * say s3 crashes then w1 can send using s2 to s4 only when the s2 original w2 batch data coing from s2 is finished,
	 * only after that the s4 can sent it back on s2 and then w1 copies its received
	 * data from s2 to inside its recveived[1] matric row only and any not inside received[2]
	 * so inside recived[1] only the w1 data we store and
	 * inside received[2] only the w2 data we store.
	 * so this mutex make sure when the node is up, it processes the complete batch data of the worker and not the
	 * partial batch of the worker. This is true also in case of fault tollerence.
         */

	int reassigned = 0;
	int lock_hold = 0;
	while (offset < chunk_size ) {

		if (!lock_hold) {
			pthread_mutex_lock(&w_socket_mutexes[current_worker_index]);
		}

		lock_hold = 1;
		printf("I am at after taking mutex for current_worker_index %d & before send\n", current_worker_index);

		int bytes_to_send = (chunk_size - offset > BUFFER_SIZE) ? BUFFER_SIZE : (chunk_size - offset);
		sent_bytes = send(client_sockets[current_worker_index], &chunk[offset], bytes_to_send * sizeof(int), 0);

		// Attempt to send the chunk to the current worker
		//sent_bytes = send(client_sockets[current_worker_index], chunk, sizeof(int) * chunk_size, 0);
		printf("just after the send call the sent bytes is %d\n", sent_bytes);

	        if (sent_bytes <= 0) {
       		     // If sending fails, mark the worker as unavailable
          		 printf("Worker %d disconnected while sending the chunk. Reassigning task...\n", current_worker_index + 1);
			is_alive[current_worker_index] = 0;

			//release mutex here
			pthread_mutex_unlock(&w_socket_mutexes[current_worker_index]);
			lock_hold = 0;

			// Find another available worker to handle this chunk
			for (int i = 0; i < MAX_WORKERS; i++) {
				if (is_alive[i]) {
					printf("Reassigning chunk to worker node %d\n", i + 1);
					current_worker_index = i;

					/*
					 * sleep for some time, because might be possible we have just sent chunk to 
					 * this worker socket index
					 * just now and again go to send immediately in that case it might club both the chunk
					 * at receive
					 * side we don't want that, so sleep for very small time.
					 */
					reassigned = 1;
					offset = 0;
					break;
				}
			}
			printf("hi1 after finding fault tollerence node\n");
			// If no workers are available, exit the function
			if (!reassigned) {
				printf("No available workers nodes to handle the chunk. Exiting task.\n");
				pthread_exit(NULL);
			}
			usleep(delay_time);
			//continue;  // Retry sending to the new worker
			break;

		}//if fault
		offset += sent_bytes / sizeof(int);

	}//while offset < chunk_s

	if (reassigned) {
		continue;
	}

	//now inform the client node that the chunk is fully sent
	int end_signal = -1;
	send(client_sockets[current_worker_index], &end_signal, sizeof(int), 0);

	//now receive the sorted chunk in smaller pages
	printf("I am at after send and before recv & after taking mutex for current_worker_index %d & before recv\n", current_worker_index);

	offset = 0;
	while (offset < chunk_size) {

		int bytes_to_receive = (chunk_size - offset > BUFFER_SIZE) ? BUFFER_SIZE : (chunk_size - offset);
		received_bytes = recv(client_sockets[current_worker_index], &received_chunks[initial_worker_index][offset], bytes_to_receive * sizeof(int), 0);

		// Receive the sorted chunk back from the worker
		//received_bytes = read(client_sockets[current_worker_index], received_chunks[initial_worker_index], sizeof(int) * chunk_size);
		printf("I am just after recv, recv returned value is %d\n", received_bytes);

		if (received_bytes <= 0) {
			// If receiving fails, mark the worker as unavailable
			printf("Worker %d disconnected during reception. Reassigning task...\n", current_worker_index + 1);
			is_alive[current_worker_index] = 0;
			//release mutex
			pthread_mutex_unlock(&w_socket_mutexes[current_worker_index]);
			lock_hold = 0;

			// Find another available worker to handle this chunk
			for (int i = 0; i < MAX_WORKERS; i++) {
				if (is_alive[i]) {
					printf("Reassigning chunk to worker node %d\n", i + 1);
					current_worker_index = i;
					//pthread_mutex_lock(&w_socket_mutexes[current_worker_index]);
					reassigned = 1;
					offset = 0;
					break;
				}
			}
			// If no workers are available, exit the function
			if (!reassigned) {
				printf("No available workers nodes to handle the chunk. Exiting task.\n");
				pthread_exit(NULL);
			}
			printf("I am here2\n");
			usleep(delay_time);
			//continue;  // Retry receiving from the reassigned worker
			break; //again start to send from fresh
		}
		offset += received_bytes / sizeof(int);

	}//while offset < chunk_size

	if (reassigned) {
		continue;
	}

        printf("Worker %d has successfully sorted the chunk and sent it back.\n", current_worker_index + 1);
       
	for (int i = 0; i < chunk_size; i++) {
		printf(" %d ", received_chunks[initial_worker_index][i]);
	}
	printf("\n");

        pthread_mutex_unlock(&w_socket_mutexes[current_worker_index]);

        pthread_mutex_lock(&chunks_received_mutex);
        chunks_received++;  // Increment the counter when a chunk is received
        pthread_mutex_unlock(&chunks_received_mutex);

	// If both send and receive are successful, break the loop
        break;

    }//worker loop

    return NULL;
}


// Merge all received sorted chunks
void merge_chunks(int num_chunks, int *chunks[], int chunk_sizes[], int total_size) {
    int *merged_data = NULL;
    int *indexes = NULL;
    FILE *output_file = fopen("output.txt", "w");
    if (output_file == NULL) {
        perror("Error opening output file");
        return;
    }

    merged_data = (int *)malloc(total_size * sizeof(int));
    if (merged_data == NULL ) {
        printf("malloc failed! exiting\n");
        exit(1);
    }
    indexes = (int *)calloc(num_chunks, sizeof(int));
    if (indexes == NULL) {
        printf("calloc failed, exiting\n");
        exit(1);
    }
    for (int i = 0; i < total_size; i++) {
        int min_val = __INT_MAX__;
        int min_index = -1;

        for (int j = 0; j < num_chunks; j++) {
            if (indexes[j] < chunk_sizes[j] && chunks[j][indexes[j]] < min_val) {
                min_val = chunks[j][indexes[j]];
                min_index = j;
            }
        }

        if (min_index != -1) {
            merged_data[i] = min_val;
            indexes[min_index]++;
        }
    }

    for (int i = 0; i < total_size; i++) {
        fprintf(output_file, "%d\n", merged_data[i]);
    }

    free(merged_data);
    free(indexes);
    fclose(output_file);
}

