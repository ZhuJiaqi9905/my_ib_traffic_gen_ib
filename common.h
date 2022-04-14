#ifndef COMMON_H
#define COMMON_H

#include <infiniband/verbs.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/time.h>
#define DEFAULT_IB_PORT 1

#define DEFAULT_MSG_SIZE 4096

#define DEFAULT_ITERS 100

#define DEFAULT_SERVER_PORT 12345

#define DEFAULT_CONTROLLER_PORT 12345

#define DEFAULT_NUM_QPS 1

#define DEFAULT_TX_DEPTH 1

#define DEFAULT_RX_DEPTH 50

#define COMPLETE_MSG "Complete"

#define READY_MSG "Ready"

#define MAX_GID_COUNT 128

enum {
  RECV_WRID = 1,
  SEND_WRID = 2,
  WRITE_WRID = 3,
};

struct dev_context {
  // IB device name
  char *ib_dev_name;
  // IB device port
  int dev_port;

  // Global identifier
  int gid_index_list[MAX_GID_COUNT];
  union ibv_gid gid_list[MAX_GID_COUNT];
  size_t gid_count;

  // GUID
  uint64_t guid;

  // IB device context
  struct ibv_context *ctx;
  // IB device attribute
  struct ibv_device_attr dev_attr;
  // IB port attribute
  struct ibv_port_attr port_attr;

  // Completion channel
  struct ibv_comp_channel *channel;
  // Protection domain
  struct ibv_pd *pd;
  // Completion queue
  struct ibv_cq *cq;
  // If use completion channel (event driven)
  bool use_event;
};

// Connection destination information
struct conn_dest {
  // Local identifier
  uint16_t lid;
  // Queue pair number
  uint32_t qpn;
  // Packet sequence number
  uint32_t psn;
  // Global identifier
  union ibv_gid gid;
  // GUID
  uint64_t guid;
};

// Memory information
struct conn_mem {
  uint64_t addr;
  uint32_t key;
} __attribute__((packed));

struct conn_context {
  unsigned int id;
  struct dev_context *dev_ctx;
  // Queue pair
  struct ibv_qp *qp;

  // Memory region for data
  struct ibv_mr *data_mr;

  // Memory for data
  unsigned char *data_buf;
  size_t data_buf_size;
  bool validate_buf;

  // Work request send flags
  bool inline_msg;
  int send_flags;

  // Destination information
  struct conn_dest local_dest;
  // Remote Destination information
  struct conn_dest rem_dest;

  // Remote memory information
  struct conn_mem rem_mem;

  unsigned int gid_index;
  bool use_multi_gid;

  // Remote GUID
  uint64_t rem_guid;

  // Statistics
  unsigned int post_reqs;
  unsigned int complete_reqs;
  struct timeval end;
};

// Initialize device context
bool init_dev_ctx(struct dev_context *ctx);

// Destory device context
void destroy_dev_ctx(struct dev_context *ctx);

// Print device context
void print_dev_ctx(struct dev_context *ctx);

// Initialize connection context
bool init_conn_ctx(struct conn_context *ctx);

// Destroy connection context
void destroy_conn_ctx(struct conn_context *ctx);

// Print connection context
void print_conn_ctx(struct conn_context *ctx);

// Print destination information
void print_dest(struct conn_dest *dest);

// Print memory information
void print_mem(struct conn_mem *mem);

// Connect QP with a remote destination
// Return true on success.
bool connect_qp(struct conn_context *ctx, struct conn_dest *dest);

// Post 'n' write requests.
// Return # of write requests that are successfully posted.
unsigned int post_write(struct conn_context *ctx, unsigned int n);

// Post 'n' send requests.
// Return # of send requests that are successfully posted.
unsigned int post_send(struct conn_context *ctx, unsigned int n);

// Post 'n' receive requests.
// Return # of receive requests that are successfully posted.
unsigned int post_recv(struct conn_context *ctx, unsigned int n);

// Wait for a completed work request.
// Return the completed work request in @wc.
// Return true on success.
bool wait_for_wc(struct ibv_cq *cq, struct ibv_wc *wc);

// Parse a RDMA write completion
bool parse_write_wc(struct ibv_wc *wc);

// Parse a RDMA send completion
bool parse_send_wc(struct ibv_wc *wc);

// Parse a RDMA receive completion
bool parse_recv_wc(struct ibv_wc *wc);

// Write exactly 'count' bytes storing in buffer 'buf' into
// the file descriptor 'fd'.
// Return the number of bytes sucsessfully written.
size_t write_exact(int fd, char *buf, size_t count);

// Read exactly 'count' bytes from the file descriptor 'fd'
// and store the bytes into buffer 'buf'.
// Return the number of bytes successfully read.
size_t read_exact(int fd, char *buf, size_t count);

// Build a socket connection with ip:port
int connect_socket(char *ip, uint16_t port);

// Start a TCP socket server
int start_socket_server(uint16_t listen_port);

// Accept an connection from the client
int accept_connection(int serv_sockfd);

// Notify the controller of qpns and psns
bool notify_controller(char *ip, uint16_t port,
                       struct conn_context *connections, unsigned int num_qps);

// Validate buffer content
bool validate_buffer(struct conn_context *connections, unsigned int num_qps);

// Exchange RDMA metadata with the server
bool exchange_metadata_with_server(int serv_sockfd,
                                   struct conn_context *connections,
                                   unsigned int num_qps, bool exchange_memory);

// Exchange RDMA metadata with the client
bool exchange_metadata_with_client(int cli_sockfd,
                                   struct conn_context *connections,
                                   unsigned int num_qps, bool exchange_memory);

#endif
