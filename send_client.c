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
static bool parse_args(int argc, char **argv);
// Send iters to the server
static bool send_iters(int serv_sockfd);
// Wait for the ready notification from the server
static bool wait_ready(int serv_sockfd);
// Generate RDMA Send traffic
static bool gen_traffic(struct conn_context *connections,
                        unsigned int num_qps,
                        unsigned int iters,
                        unsigned int tx_depth,
                        bool use_event);

static uint16_t server_port      = DEFAULT_SERVER_PORT;
static char *ib_dev_name         = NULL;
static int ib_port               = DEFAULT_IB_PORT;
static unsigned int msg_size     = DEFAULT_MSG_SIZE;
static unsigned int iters        = DEFAULT_ITERS;
static unsigned int num_qps      = DEFAULT_NUM_QPS;
static unsigned int tx_depth     = DEFAULT_TX_DEPTH;
static bool inline_msg           = false;
static bool use_event            = false;
static bool validate_buf         = false;
static bool use_multi_gid        = false;
static char *server_ip           = NULL;
static char *controller_ip       = NULL;
static uint16_t controller_port  = DEFAULT_CONTROLLER_PORT;

int main(int argc, char **argv)
{
    int server_sockfd = -1;

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
        .use_event = use_event
    };

    if (!init_dev_ctx(&device)) {
        fprintf(stderr, "Fail to initialize device context\n");
        return EXIT_FAILURE;
    }
    print_dev_ctx(&device);

    unsigned int tx_depth_limit = (unsigned int)(device.dev_attr.max_qp_wr) / 4;
    if (tx_depth > tx_depth_limit) {
        fprintf(stderr, "TX depth %u > limit %u\n", tx_depth, tx_depth_limit);
        goto destroy_device;
    }

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
        connections[i].validate_buf = validate_buf;
        connections[i].inline_msg = inline_msg;
        connections[i].use_multi_gid = use_multi_gid;

        if (!init_conn_ctx(&connections[i])) {
            fprintf(stderr, "Fail to initialize connection %u\n", i);
            goto destroy_connections;
        }

        num_qps_init++;
        print_conn_ctx(&connections[i]);
    }

    // Build a TCP connection to the server
    server_sockfd = connect_socket(server_ip, server_port);
    if (server_sockfd < 0) {
        fprintf(stderr, "Fail to connect to the server\n");
        goto destroy_connections;
    }

    // Exchange metadata with server
    if (!exchange_metadata_with_server(server_sockfd, connections, num_qps, false)) {
        fprintf(stderr, "Fail to exchange RDMA metadata with the server\n");
        goto destroy_socket;
    }
    printf("Exchange RDMA metadata with the server\n");

    // Send iters to the server
    if (!send_iters(server_sockfd)) {
        fprintf(stderr, "Fail to send iters to the server\n");
        goto destroy_socket;
    }
    printf("Send iters to the server\n");

    // Wait for ready message from server
    if (!wait_ready(server_sockfd)) {
        fprintf(stderr, "Fail to get ready message from the server\n");
        goto destroy_socket;
    }
    printf("The server is ready\n");
    close(server_sockfd);

    if (controller_ip &&
        !notify_controller(controller_ip, controller_port, connections, num_qps)) {
        fprintf(stderr, "Fail to notify the controller\n");
    }

    if (!gen_traffic(connections, num_qps, iters, tx_depth, use_event)) {
        fprintf(stderr, "Fail to generate traffic\n");
        goto destroy_connections;
    }

    printf("Experiment completes\n");
    for (unsigned int i = 0; i < num_qps; i++) {
        destroy_conn_ctx(&connections[i]);
    }

    free(connections);
    destroy_dev_ctx(&device);
    return EXIT_SUCCESS;

destroy_socket:
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
    fprintf(stderr, "Usage: %s [options] host\n", app);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -p, --port=<port>            listen on/connect to port <port> (default %d)\n", DEFAULT_SERVER_PORT);
    fprintf(stderr, "  -d, --ib-dev=<dev>           use IB device <dev>\n");
    fprintf(stderr, "  -i, --ib-port=<port>         use port <port> of IB device (default %d)\n", DEFAULT_IB_PORT);
    fprintf(stderr, "  -C, --controller=<ip>        IP address of the controller\n");
    fprintf(stderr, "  -P, --controller-port=<port> connect to port <port> of controller (default %d)\n", DEFAULT_CONTROLLER_PORT);
    fprintf(stderr, "  -s, --size=<size>            size of message to exchange (default %d)\n", DEFAULT_MSG_SIZE);
    fprintf(stderr, "  -n, --iters=<iters>          number of exchanges per queue pair (default %d)\n", DEFAULT_ITERS);
    fprintf(stderr, "  -q, --qp=<num of qp's>       number of queue pairs (default %d)\n", DEFAULT_NUM_QPS);
    fprintf(stderr, "  -t  --tx-depth=<dep>         size of tx queue of each queue pair (default %d)\n", DEFAULT_TX_DEPTH);
    fprintf(stderr, "  -l, --inline                 inline message with the work request\n");
    fprintf(stderr, "  -e, --events                 sleep on CQ events\n");
    fprintf(stderr, "  -c, --chk                    validate received buffer\n");
    fprintf(stderr, "  -m, --multi-gid              use multiple GIDs associated with the IB interface\n");
    fprintf(stderr, "  -h, --help                   show this help screen\n");
}

static bool parse_args(int argc, char **argv)
{
    while (1) {
        static struct option long_options[] = {
            { .name = "port",            .has_arg = 1, .val = 'p' },
            { .name = "ib-dev",          .has_arg = 1, .val = 'd' },
            { .name = "ib-port",         .has_arg = 1, .val = 'i' },
            { .name = "controller",      .has_arg = 1, .val = 'C' },
            { .name = "controller-port", .has_arg = 1, .val = 'P' },
            { .name = "size",            .has_arg = 1, .val = 's' },
            { .name = "iters",           .has_arg = 1, .val = 'n' },
            { .name = "qps",             .has_arg = 1, .val = 'q' },
            { .name = "tx-depth",        .has_arg = 1, .val = 't' },
            { .name = "inline",          .has_arg = 0, .val = 'l' },
            { .name = "events",          .has_arg = 0, .val = 'e' },
            { .name = "chk",             .has_arg = 0, .val = 'c' },
            { .name = "multi-gid",       .has_arg = 0, .val = 'm' },
            { .name = "help",            .has_arg = 0, .val = 'h' },
            {}
        };

        int c = getopt_long(argc, argv, "p:d:i:C:P:s:n:q:t:lecmh", long_options, NULL);

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

            case 'C':
                controller_ip = optarg;
                break;

            case 'P':
                controller_port = (uint16_t)strtoul(optarg, NULL, 0);
                break;

            case 'n':
			    iters = (unsigned int)strtoul(optarg, NULL, 0);
			    break;

            case 'q':
                num_qps = (unsigned int)strtoul(optarg, NULL, 0);
                break;

            case 't':
                tx_depth = (unsigned int)strtoul(optarg, NULL, 0);
                break;

            case 'l':
                inline_msg = true;
                break;

            case 'e':
                use_event = true;
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

    //printf("optind %d argc %d\n", optind, argc);
    if (optind == argc - 1) {
        server_ip = argv[optind];
    }

    if (!server_ip || !ib_dev_name) {
        if (!server_ip) {
            fprintf(stderr, "Fail to get host\n");
        }
        if (!ib_dev_name) {
            fprintf(stderr, "Fail to get IB device\n");
        }
        print_usage(argv[0]);
        return false;
    }

    return true;
}

static bool gen_traffic(struct conn_context *connections,
                        unsigned int num_qps,
                        unsigned int iters,
                        unsigned int tx_depth,
                        bool use_event)
{
    unsigned int init_num_reqs = (tx_depth < iters)? tx_depth : iters;
    // # of QPs that have compeleted all the requests
    unsigned int num_complete_qps = 0;
    struct ibv_cq *cq = connections[0].dev_ctx->cq;
    struct ibv_comp_channel *channel = connections[0].dev_ctx->channel;
    struct ibv_wc wc;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    int ne;
    struct timeval start, end;

    if (gettimeofday(&start, NULL)) {
		fprintf(stderr, "Cannot get current time\n");
		return false;
	}

    for (unsigned int i = 0; i < num_qps; i++) {
        if (post_send(&connections[i], init_num_reqs) != init_num_reqs) {
            fprintf(stderr, "Could not post %u send on QP %u\n", init_num_reqs, i);
            return false;
        }
        connections[i].post_reqs = init_num_reqs;
        connections[i].complete_reqs = 0;
    }

    // If any QPs have not completed all the requests
    while (num_complete_qps < num_qps) {
        // Wait for completion events.
        // If we use busy polling, this step is skipped.
        if (use_event) {
            if (ibv_get_cq_event(channel, &ev_cq, &ev_ctx)) {
				fprintf(stderr, "Fail to get cq_event\n");
				return false;
			}

            if (ev_cq != cq) {
                fprintf(stderr, "CQ event for unknown CQ %p\n", ev_cq);
				return false;
            }

            ibv_ack_cq_events(cq, 1);

            if (ibv_req_notify_cq(cq, 0)) {
                fprintf(stderr, "Cannot request CQ notification\n");
                return false;
            }
        }

        // Empty the completion queue
        while (true) {
            ne = ibv_poll_cq(cq, 1, &wc);
            if (ne < 0) {
                fprintf(stderr, "Fail to poll CQ (%d)\n", ne);
			    return false;

            } else if (ne == 0) {
                break;
            }

            if (!parse_send_wc(&wc)) {
                fprintf(stderr, "Fail to get the completion event\n");
                return false;
            }

            unsigned int qp = wc.wr_id;
            connections[qp].complete_reqs++;

            if (connections[qp].complete_reqs == iters) {
                num_complete_qps++;
                if (gettimeofday(&(connections[qp].end), NULL)) {
                    fprintf(stderr, "Cannot get current time\n");
		            return false;
                }
            } else if (connections[qp].post_reqs < iters) {
                if (post_send(&connections[qp], 1) != 1) {
                    fprintf(stderr, "Could not post send on QP %u\n", qp);
                    return false;
                }
                connections[qp].post_reqs++;
            }
        }
    }

    if (gettimeofday(&end, NULL)) {
		fprintf(stderr, "Cannot get current time\n");
		return false;
	}

    float total_usec = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    float total_bytes = num_qps * iters * connections[0].data_buf_size;
    float tput_gbps = total_bytes * 8 / total_usec / 1e3;
    printf("Throughput: %.2f Gbps\n", tput_gbps);

    for (unsigned int i = 0; i < num_qps; i++) {
        printf("QP %u: post %u requests, complete %u requests",
                i, connections[i].post_reqs, connections[i].complete_reqs);

        if (tx_depth == 1) {
            float usec = (connections[i].end.tv_sec - start.tv_sec) * 1000000 + \
                         (connections[i].end.tv_usec - start.tv_usec);
            printf(", %.2f usec/iter\n", usec / iters);
        } else {
            printf("\n");
        }
    }

    return true;
}

// Send iters to the server
static bool send_iters(int serv_sockfd)
{
    size_t buf_size = sizeof(iters);
    return write_exact(serv_sockfd, (char*)&iters, buf_size) == buf_size;
}

// Wait for the ready notification from the server
static bool wait_ready(int serv_sockfd)
{
    char buf[] = READY_MSG;
    size_t buf_size = sizeof(buf);
    return read_exact(serv_sockfd, buf, buf_size) == buf_size;
}
