#define _GNU_SOURCE
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/time.h>
#include <getopt.h>
#include <string.h>
#include "common.h"

static void print_usage(char *app);
static bool parse_args(int argc, char **argv);;
// Wait for the completion notification from the client
static bool wait_completion(int cli_sockfd);

static uint16_t server_port     = DEFAULT_SERVER_PORT;
static char *ib_dev_name        = NULL;
static int ib_port              = DEFAULT_IB_PORT;
static unsigned int msg_size    = DEFAULT_MSG_SIZE;
static unsigned int num_qps     = DEFAULT_NUM_QPS;
static bool validate_buf        = false;
static bool use_multi_gid       = false;

int main(int argc, char **argv)
{
    int server_sockfd = -1;
    int client_sockfd = -1;

    if (!parse_args(argc, argv)) {
        return EXIT_FAILURE;
    }

    struct dev_context device = {
        .ib_dev_name = ib_dev_name,
        .dev_port = ib_port,
        .ctx = NULL,
        .channel = NULL,
        .pd = NULL,
        .cq = NULL,
        .use_event = false
    };

    if (!init_dev_ctx(&device)) {
        fprintf(stderr, "Fail to initialize device context\n");
        return EXIT_FAILURE;
    }
    print_dev_ctx(&device);

    printf("Initialize %u queue pairs\n", num_qps);
    unsigned int num_qps_init = 0;
    struct conn_context *connections = (struct conn_context*)malloc(num_qps * sizeof(struct conn_context));
    if (!connections) {
        goto destroy_device;
    }

    for (unsigned int i = 0; i < num_qps; i++) {
        connections[i].id = i;
        connections[i].dev_ctx = &device;
        connections[i].qp = NULL;
        connections[i].data_mr = NULL;
        connections[i].data_buf = NULL;
        connections[i].data_buf_size = msg_size;
        connections[i].validate_buf = false;
        connections[i].inline_msg = false;
        connections[i].use_multi_gid = use_multi_gid;

        if (!init_conn_ctx(&connections[i])) {
            fprintf(stderr, "Fail to initialize connection %u\n", i);
            goto destroy_connections;
        }

        num_qps_init++;
        print_conn_ctx(&connections[i]);
    }

    server_sockfd = start_socket_server(server_port);
    if (server_sockfd < 0) {
        fprintf(stderr, "Fail to initialize socket server on port %hu\n", server_port);
        goto destroy_connections;
    }

    client_sockfd = accept_connection(server_sockfd);
    if (client_sockfd < 0) {
        fprintf(stderr, "Fail to accept an connection\n");
        goto destroy_socket_server;
    }

    if (!exchange_metadata_with_client(client_sockfd, connections, num_qps, true)) {
        fprintf(stderr, "Fail to exchange RDMA metadata with the client\n");
        goto destroy_socket_client;
    }
    printf("Exchange RDMA metadata with the client\n");

    if (!wait_completion(client_sockfd)) {
        fprintf(stderr, "Fail to get the completion notification\n");
        goto destroy_socket_client;
    }

    close(client_sockfd);
    close(server_sockfd);

    printf("Experiment completes\n");
    if (validate_buf) {
        if (!validate_buffer(connections, num_qps)) {
            fprintf(stderr, "Invalid buffer content\n");
            goto destroy_connections;
        } else {
            printf("Validate buffer\n");
        }
    }

    for (unsigned int i = 0; i < num_qps; i++) {
        destroy_conn_ctx(&connections[i]);
    }

    free(connections);
    destroy_dev_ctx(&device);
    return EXIT_SUCCESS;

destroy_socket_client:
    close(client_sockfd);

destroy_socket_server:
    close(server_sockfd);

destroy_connections:
    for (unsigned int i = 0; i < num_qps_init; i++) {
        destroy_conn_ctx(&connections[i]);
    }
    free(connections);

destroy_device:
    destroy_dev_ctx(&device);
    return EXIT_FAILURE;
}

static void print_usage(char *app)
{
    if (!app) {
        return;
    }

    fprintf(stderr, "Usage: %s [options]\n", app);
    fprintf(stderr, "Options:\n");

    fprintf(stderr, "  -p, --port=<port>         listen on/connect to port <port> (default %d)\n", DEFAULT_SERVER_PORT);
    fprintf(stderr, "  -d, --ib-dev=<dev>        use IB device <dev>\n");
    fprintf(stderr, "  -i, --ib-port=<port>      use port <port> of IB device (default %d)\n", DEFAULT_IB_PORT);
    fprintf(stderr, "  -s, --size=<size>         size of message to exchange (default %d)\n", DEFAULT_MSG_SIZE);
    fprintf(stderr, "  -q, --qp=<num of qp's>    number of queue pairs (default %d)\n", DEFAULT_NUM_QPS);
    fprintf(stderr, "  -c, --chk                 validate received buffer\n");
    fprintf(stderr, "  -m, --multi-gid           use multiple GIDs associated with the IB interface\n");
    fprintf(stderr, "  -h, --help                show this help screen\n");
}

static bool parse_args(int argc, char **argv)
{
    while (1) {
        static struct option long_options[] = {
            { .name = "port",     .has_arg = 1, .val = 'p' },
            { .name = "ib-dev",   .has_arg = 1, .val = 'd' },
            { .name = "ib-port",  .has_arg = 1, .val = 'i' },
            { .name = "size",     .has_arg = 1, .val = 's' },
            { .name = "qps",      .has_arg = 1, .val = 'q' },
            { .name = "chk",      .has_arg = 0, .val = 'c' },
            { .name = "multi-gid",.has_arg = 0, .val = 'm' },
            { .name = "help",     .has_arg = 0, .val = 'h' },
            {}
        };

        int c = getopt_long(argc, argv, "p:d:i:s:q:cmh", long_options, NULL);

        //printf("%d\n", c);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'p':
                server_port = (uint16_t)strtoul(optarg, NULL, 0);
                break;

            case 'd':
                ib_dev_name = optarg;
                break;

            case 'i':
                ib_port = (int)strtol(optarg, NULL, 0);
                if (ib_port < 1) {
                    print_usage(argv[0]);
                    return false;
                }
                break;

            case 's':
                msg_size = (unsigned int)strtoul(optarg, NULL, 0);
                break;

            case 'q':
                num_qps = (unsigned int)strtoul(optarg, NULL, 0);
                break;

            case 'c':
                validate_buf = true;
                break;

            case 'm':
                use_multi_gid = true;
                break;

            case 'h':
            default:
                print_usage(argv[0]);
                return false;
        }
    }

    if (!ib_dev_name) {
        fprintf(stderr, "Fail to get IB device\n");
        print_usage(argv[0]);
        return false;
    }

    return true;
}

// Wait for the completion notification from the client
static bool wait_completion(int cli_sockfd)
{
    char buf[sizeof(COMPLETE_MSG)] = {0};
    size_t buf_size = sizeof(COMPLETE_MSG);
    return read_exact(cli_sockfd, buf, buf_size) == buf_size && strcmp(buf, COMPLETE_MSG) == 0;
}
