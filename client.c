#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "client.h"

#define PORT 9009 
#define BUFFER_SIZE 1024
#define CHUNK_SIZE 4096

void merge(int *data, int left, int mid, int right);
void merge_sort(int *data, int left, int right);

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
        fscanf(fp, "%s", buffer);
        token = strtok(buffer, "=\n");
        if (strcmp(token, "SERVER_IP")) {
                printf("Invalid string in client.conf file, it should be SERVER_IP=<ip-adress>\n");
                goto out;
        }
        while(token != NULL) {
                token = strtok(NULL, "=\n");
                if (token == NULL) break;
                memcpy(server_ip, token, strlen(token));
        }
        memset(buffer, 0, sizeof(buffer));
        fscanf(fp, "%s", buffer);
        token = strtok(buffer, "=\n");
        if (strcmp(token, "SERVER_PORT")) {
                printf("Invalid string in client.conf file, it should be SERVER_PORT=<server-port-number\n");
                goto out;
        }
        while(token != NULL) {
                token = strtok(NULL, "=\n");
                if (token == NULL) break;
                server_port = atoi(token);
        }

out:
        fclose(fp);

        return;
}


int main(int argc, char *argv[]) {
    int client_socket;
    struct sockaddr_in server_addr;

    if (argc != 2) {
            printf("Please provide client.conf as input of client program\n");
            exit(1);
    }
    read_conf_file(argv[1]);


    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    //server_addr.sin_addr.s_addr = INADDR_ANY;
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        printf("Error in inet_pton\n");
        exit(1);
    }

    if (connect(client_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Connection to server failed");
        close(client_socket);
        exit(EXIT_FAILURE);
    }

    printf("Connected to server at IP: %s PORT: %d\n", server_ip, server_port);

    int buffer[BUFFER_SIZE];
    int chunk[CHUNK_SIZE];
    int chunk_size = 0;
    int num_integers = 0;
    while (1) {

        int received_size = recv(client_socket, buffer, sizeof(buffer), 0);
        if (received_size <= 0) {
            printf("Connection closed by server\n");
            break;
        }

        // Calculate the number of integers received
        int buf_count_int = received_size / sizeof(int);
        printf("Received %d integers to sort\n", buf_count_int);

        for (int i = 0; i < buf_count_int; i++) {
                printf(" %d ", buffer[i]);
        }
        printf("\n");
	num_integers += buf_count_int;

	for (int i = 0; i < received_size / sizeof(int); i++) {
		if (buffer[i] == -1) {
			num_integers--;//reduce one count for -1
			// Send the sorted data back to the server
			printf("Total Received %d integers to sort\n", num_integers);
			merge_sort(chunk, 0, num_integers - 1);

			send(client_socket, chunk, chunk_size * sizeof(int), 0);
			for (int i = 0; i < num_integers; i++) {
				printf(" %d ", chunk[i]);
			}
			printf("\n");
			chunk_size = 0;
			num_integers = 0;
		} else {
			chunk[chunk_size++] = buffer[i];
		}
	}

	memset(buffer, 0, sizeof(buffer));

	//usleep(5000000);
    }

    close(client_socket);
    return 0;
}

void merge(int *data, int left, int mid, int right) {
    int n1 = mid - left + 1;
    int n2 = right - mid;

    int *leftArr = malloc(n1 * sizeof(int));
    int *rightArr = malloc(n2 * sizeof(int));

    for (int i = 0; i < n1; i++) leftArr[i] = data[left + i];
    for (int i = 0; i < n2; i++) rightArr[i] = data[mid + 1 + i];

    int i = 0, j = 0, k = left;
    while (i < n1 && j < n2) {
        if (leftArr[i] <= rightArr[j]) {
            data[k++] = leftArr[i++];
        } else {
            data[k++] = rightArr[j++];
        }
    }

    while (i < n1) data[k++] = leftArr[i++];
    while (j < n2) data[k++] = rightArr[j++];

    free(leftArr);
    free(rightArr);
}

void merge_sort(int *data, int left, int right) {
    if (left < right) {
        int mid = left + (right - left) / 2;
        merge_sort(data, left, mid);
        merge_sort(data, mid + 1, right);
        merge(data, left, mid, right);
    }
}

