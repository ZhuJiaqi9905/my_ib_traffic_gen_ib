#include "common.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <inttypes.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// In a list of IB devices (dev_list), given a IB device's name
// (ib_dev_name), the function returns its ID.
static inline int ib_dev_id_by_name(char *ib_dev_name,
                                    struct ibv_device **dev_list,
                                    int num_devices) {
  for (int i = 0; i < num_devices; i++) {
    if (strcmp(ibv_get_device_name(dev_list[i]), ib_dev_name) == 0) {
      return i;
    }
  }

  return -1;
}



// Initialize device context
bool init_dev_ctx(struct dev_context *ctx) {
  if (!ctx || !(ctx->ib_dev_name)) {
    goto err;
  }

  struct ibv_device **dev_list = NULL;
  int num_devices;

  // Get IB device list
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    fprintf(stderr, "Fail to get IB device list\n");
    goto err;

  } else if (num_devices == 0) {
    fprintf(stderr, "No IB devices found\n");
    goto clean_dev_list;
  }

  int ib_dev_id = -1;
  ib_dev_id = ib_dev_id_by_name(ctx->ib_dev_name, dev_list, num_devices);
  if (ib_dev_id < 0) {
    fprintf(stderr, "Fail to find IB device %s\n", ctx->ib_dev_name);
    goto clean_dev_list;
  }

  // Create a context for the RDMA device
  ctx->ctx = ibv_open_device(dev_list[ib_dev_id]);
  if (ctx->ctx) {
    printf("Open IB device %s\n", ibv_get_device_name(dev_list[ib_dev_id]));
  } else {
    fprintf(stderr, "Fail to open IB device %s\n",
            ibv_get_device_name(dev_list[ib_dev_id]));
    goto clean_dev_list;
  }

  // Get GUID (Node global unique identifier)
  //   ctx->guid = ibv_get_device_guid(dev_list[ib_dev_id]);

  // Get the index of GIDs whose types are RoCE v2
  //   get_rocev2_gid_index(ctx);
  //   if (ctx->gid_count == 0) {
  //     fprintf(stderr, "Cannot find any RoCE v2 GID\n");
  //     goto clean_device;
  //   }

  // Get RoCE v2 GIDs
  //   for (size_t i = 0; i < ctx->gid_count; i++) {
  //     if (ibv_query_gid(ctx->ctx, ctx->dev_port, ctx->gid_index_list[i],
  //                       &(ctx->gid_list[i])) != 0) {
  //       fprintf(stderr, "Cannot read GID of index %d\n",
  //       ctx->gid_index_list[i]); goto clean_device;
  //     }
  //   }

  // Create a completion channel
  if (ctx->use_event) {
    ctx->channel = ibv_create_comp_channel(ctx->ctx);
    if (!(ctx->channel)) {
      fprintf(stderr, "Cannot create completion channel\n");
      goto clean_device;
    }
  } else {
    ctx->channel = NULL;
  }

  // Allocate protection domain
  ctx->pd = ibv_alloc_pd(ctx->ctx);
  if (!(ctx->pd)) {
    fprintf(stderr, "Fail to allocate protection domain\n");
    goto clean_comp_channel;
  }

  // Query device attributes
  if (ibv_query_device(ctx->ctx, &(ctx->dev_attr)) != 0) {
    fprintf(stderr, "Fail to query device attributes\n");
    goto clean_pd;
  }

  // Query port attributes
  if (ibv_query_port(ctx->ctx, ctx->dev_port, &(ctx->port_attr)) != 0) {
    fprintf(stderr, "Fail to query port attributes\n");
    goto clean_pd;
  }

  // Create a completion queue
  printf("max cqe is %d\n", ctx->dev_attr.max_cqe);
  printf("max wre is %d\n", ctx->dev_attr.max_qp_wr);
  ctx->cq = ibv_create_cq(ctx->ctx, ctx->dev_attr.max_cqe - 10, NULL,
                          ctx->channel, 0);
  if (!(ctx->cq)) {
    fprintf(stderr, "Fail to create the completion queue\n");
    goto clean_pd;
  }

  if (ctx->use_event) {
    if (ibv_req_notify_cq(ctx->cq, 0)) {
      fprintf(stderr, "Cannot request CQ notification\n");
      goto clean_cq;
    }
  }

  ibv_free_device_list(dev_list);
  return true;

clean_cq:
  ibv_destroy_cq(ctx->cq);

clean_pd:
  ibv_dealloc_pd(ctx->pd);

clean_comp_channel:
  if (ctx->channel) {
    ibv_destroy_comp_channel(ctx->channel);
  }

clean_device:
  ibv_close_device(ctx->ctx);

clean_dev_list:
  ibv_free_device_list(dev_list);

err:
  return false;
}

// Destory device context
void destroy_dev_ctx(struct dev_context *ctx) {
  if (!ctx) {
    return;
  }

  // Destroy completion queue
  if (ctx->cq) {
    ibv_destroy_cq(ctx->cq);
  }

  // Destroy protection domain
  if (ctx->pd) {
    ibv_dealloc_pd(ctx->pd);
  }

  // Desotry completion channel
  if (ctx->channel) {
    ibv_destroy_comp_channel(ctx->channel);
  }

  // Close RDMA device context
  if (ctx->ctx) {
    ibv_close_device(ctx->ctx);
  }
}

// Print device context
void print_dev_ctx(struct dev_context *ctx) {
  if (!ctx) {
    return;
  }

  printf("Device name: %s\n", ctx->ib_dev_name);
  printf("Device port: %d\n", ctx->dev_port);
  printf("RoCE v2 GID count: %lu (Index:", ctx->gid_count);

  for (size_t i = 0; i < ctx->gid_count; i++) {
    printf(" %d", ctx->gid_index_list[i]);
  }
  printf(")\n");
}

// Initialize connection context
bool init_conn_ctx(struct conn_context *ctx) {
  if (!ctx || !(ctx->dev_ctx)) {
    goto err;
  }

  // Allocate memory
  ctx->data_buf =
      (unsigned char *)memalign(sysconf(_SC_PAGESIZE), ctx->data_buf_size);
  if (!(ctx->data_buf)) {
    fprintf(stderr, "Fail to allocate memory\n");
    goto err;
  }

  if (ctx->validate_buf) {
    for (size_t i = 0; i < ctx->data_buf_size; i++) {
      ctx->data_buf[i] = i & 0xFF;
    }
  }

  // Register memory region for data
  int access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
  ctx->data_mr = ibv_reg_mr(ctx->dev_ctx->pd, ctx->data_buf, ctx->data_buf_size,
                            access_flags);
  if (!(ctx->data_mr)) {
    fprintf(stderr, "Fail to register memory region\n");
    goto clean_data_buf;
  }

  // Create a queue pair (QP)
  struct ibv_qp_attr attr;
  struct ibv_qp_init_attr init_attr = {
      .send_cq = ctx->dev_ctx->cq,
      .recv_cq = ctx->dev_ctx->cq,
      .cap =
          {
              .max_send_wr = ctx->dev_ctx->dev_attr.max_qp_wr / 4,
              .max_recv_wr = ctx->dev_ctx->dev_attr.max_qp_wr / 4,
              .max_send_sge = 1,
              .max_recv_sge = 1,
          },
      .qp_type = IBV_QPT_RC,
  };

  ctx->qp = ibv_create_qp(ctx->dev_ctx->pd, &init_attr);
  if (!(ctx->qp)) {
    fprintf(stderr, "Fail to create QP\n");
    goto clean_data_mr;
  }

  ctx->send_flags = IBV_SEND_SIGNALED;
  if (ctx->inline_msg) {
    ibv_query_qp(ctx->qp, &attr, IBV_QP_CAP, &init_attr);

    if (init_attr.cap.max_inline_data >= ctx->data_buf_size) {
      ctx->send_flags |= IBV_SEND_INLINE;
    } else {
      fprintf(stderr,
              "Fail to set IBV_SEND_INLINE because max inline data size is %d "
              "< %ld\n",
              init_attr.cap.max_inline_data, ctx->data_buf_size);
      goto clean_qp;
    }
  }

  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = ctx->dev_ctx->dev_port;
  // Allow incoming RDMA writes on this QP
  attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;

  if (ibv_modify_qp(ctx->qp, &attr,
                    IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                        IBV_QP_ACCESS_FLAGS)) {

    fprintf(stderr, "Fail to modify QP to INIT\n");
    goto clean_qp;
  }

  srand48(getpid() * time(NULL) + ctx->id);
  // local identifier
  ctx->local_dest.lid = ctx->dev_ctx->port_attr.lid;
  // QP number
  ctx->local_dest.qpn = ctx->qp->qp_num;
  // packet sequence number
  ctx->local_dest.psn = lrand48() & 0xffffff;

  // global identifier
  unsigned int index =
      (ctx->use_multi_gid) ? ctx->id % ctx->dev_ctx->gid_count : 0;
  ctx->gid_index = ctx->dev_ctx->gid_index_list[index];
  ctx->local_dest.gid = ctx->dev_ctx->gid_list[index];

  // Get the GUID of the device
  ctx->local_dest.guid = ctx->dev_ctx->guid;

  ctx->post_reqs = 0;
  ctx->complete_reqs = 0;

  return true;

clean_qp:
  ibv_destroy_qp(ctx->qp);

clean_data_mr:
  ibv_dereg_mr(ctx->data_mr);

clean_data_buf:
  free(ctx->data_buf);

err:
  return false;
}

// Destroy connection context
void destroy_conn_ctx(struct conn_context *ctx) {
  if (!ctx) {
    return;
  }

  // Destroy queue pair
  if (ctx->qp) {
    ibv_destroy_qp(ctx->qp);
  }

  // Un-register memory region
  if (ctx->data_mr) {
    ibv_dereg_mr(ctx->data_mr);
  }

  // Free memory
  if (ctx->data_buf) {
    free(ctx->data_buf);
  }
}

// Print connection context
void print_conn_ctx(struct conn_context *ctx) {
  if (!ctx) {
    return;
  }

  char gid[33] = {0};
  inet_ntop(AF_INET6, &(ctx->local_dest.gid), gid, sizeof(gid));

  printf("Connection ID:            %u\n", ctx->id);
  printf("Data buffer size:         %lu\n", ctx->data_buf_size);
  printf("Send messages as inline:  %d\n", ctx->inline_msg);
  printf("Local identifier:         %hu\n", ctx->local_dest.lid);
  printf("Queue pair number:        %u\n", ctx->local_dest.qpn);
  printf("Packet sequence number:   %u\n", ctx->local_dest.psn);
  printf("Global Identifier:        %s\n", gid);
  printf("GUID:                     %lx\n", ctx->local_dest.guid);
}

// Print destination information
void print_dest(struct conn_dest *dest) {
  if (!dest) {
    return;
  }

  char gid[33] = {0};
  inet_ntop(AF_INET6, &(dest->gid), gid, sizeof(gid));
  printf("LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s, GUID %016lx\n", dest->lid,
         dest->qpn, dest->psn, gid, dest->guid);
}

// Print memory information
void print_mem(struct conn_mem *mem) {
  printf("Addr %" PRIu64 ", Key %" PRIu32 "\n", mem->addr, mem->key);
}

// Connect QP with a remote destination
// Return true on success.
bool connect_qp(struct conn_context *ctx, struct conn_dest *dest) {
  struct ibv_qp_attr attr = {.qp_state = IBV_QPS_RTR,
                             .path_mtu = IBV_MTU_1024,
                             // Remote QP number
                             .dest_qp_num = dest->qpn,
                             // Packet Sequence Number of the received packets
                             .rq_psn = dest->psn,
                             .max_dest_rd_atomic = 1,
                             .min_rnr_timer = 12,
                             // Address vector
                             .ah_attr = {.is_global = 0,
                                         .dlid = dest->lid,
                                         .sl = 0,
                                         .src_path_bits = 0,
                                         .port_num = ctx->dev_ctx->dev_port}};

  if (dest->gid.global.interface_id) {
    attr.ah_attr.is_global = 1;
    // Set attributes of the Global Routing Headers (GRH)
    // When using RoCE, GRH must be configured!
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.dgid = dest->gid;
    attr.ah_attr.grh.sgid_index = ctx->gid_index;
  }

  if (ibv_modify_qp(ctx->qp, &attr,
                    IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                        IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                        IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
    fprintf(stderr, "Fail to modify QP to RTR\n");
    return false;
  }

  attr.qp_state = IBV_QPS_RTS;
  // The minimum time that a QP waits for ACK/NACK from remote QP
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = ctx->local_dest.psn;
  attr.max_rd_atomic = 1;

  if (ibv_modify_qp(ctx->qp, &attr,
                    IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                        IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                        IBV_QP_MAX_QP_RD_ATOMIC)) {
    fprintf(stderr, "Failed to modify QP to RTS\n");
    return false;
  }

  return true;
}

// Post 'n' write requests.
// Return # of write requests that are successfully posted.
unsigned int post_write(struct conn_context *ctx, unsigned int n) {
  struct ibv_sge list = {.addr = (uintptr_t)(ctx->data_buf),
                         .length = ctx->data_buf_size,
                         .lkey = ctx->data_mr->lkey};

  struct ibv_send_wr wr = {.wr_id = ctx->id,
                           .sg_list = &list,
                           .num_sge = 1,
                           .opcode = IBV_WR_RDMA_WRITE,
                           .send_flags = ctx->send_flags,
                           .wr.rdma.remote_addr = ctx->rem_mem.addr,
                           .wr.rdma.rkey = ctx->rem_mem.key};

  struct ibv_send_wr *bad_wr;
  unsigned int i;
  for (i = 0; i < n; i++) {
    if (ibv_post_send(ctx->qp, &wr, &bad_wr) != 0) {
      break;
    }
  }

  return i;
}

// Post 'n' send requests.
// Return # of send requests that are successfully posted.
unsigned int post_send(struct conn_context *ctx, unsigned int n) {
  struct ibv_sge list = {.addr = (uintptr_t)(ctx->data_buf),
                         .length = ctx->data_buf_size,
                         .lkey = ctx->data_mr->lkey};

  struct ibv_send_wr wr = {.wr_id = ctx->id,
                           .sg_list = &list,
                           .num_sge = 1,
                           .opcode = IBV_WR_SEND,
                           .send_flags = ctx->send_flags};

  struct ibv_send_wr *bad_wr;
  unsigned int i;
  for (i = 0; i < n; i++) {
    if (ibv_post_send(ctx->qp, &wr, &bad_wr) != 0) {
      break;
    }
  }

  return i;
}

// Post 'n' receive requests.
// Return # of receive requests that are successfully posted.
unsigned int post_recv(struct conn_context *ctx, unsigned int n) {
  struct ibv_sge list = {.addr = (uintptr_t)(ctx->data_buf),
                         .length = ctx->data_buf_size,
                         .lkey = ctx->data_mr->lkey};

  struct ibv_recv_wr wr = {.wr_id = ctx->id, .sg_list = &list, .num_sge = 1};

  struct ibv_recv_wr *bad_wr;
  unsigned int i;
  for (i = 0; i < n; i++) {
    if (ibv_post_recv(ctx->qp, &wr, &bad_wr) != 0) {
      break;
    }
  }

  return i;
}

// Wait for a completed work request.
// Return the completed work request in @wc.
// Return true on success.
bool wait_for_wc(struct ibv_cq *cq, struct ibv_wc *wc) {
  while (true) {
    int ne = ibv_poll_cq(cq, 1, wc);
    if (ne < 0) {
      fprintf(stderr, "Fail to poll CQ (%d)\n", ne);
      return false;

    } else if (ne > 0) {
      return true;

    } else {
      // printf("Return 0\n");
      continue;
    }
  }

  // We should never reach here
  return false;
}

// Parse a RDMA write completion
bool parse_write_wc(struct ibv_wc *wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    fprintf(stderr, "Work request status is %s\n",
            ibv_wc_status_str(wc->status));
    return false;
  }

  if (wc->opcode != IBV_WC_RDMA_WRITE) {
    fprintf(stderr, "Work request opcode is not IBV_WC_RDMA_WRITE (%d)\n",
            wc->opcode);
    return false;
  }

  return true;
}

// Parse a RDMA send completion
bool parse_send_wc(struct ibv_wc *wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    fprintf(stderr, "Work request status is %s\n",
            ibv_wc_status_str(wc->status));
    return false;
  }

  if (wc->opcode != IBV_WC_SEND) {
    fprintf(stderr, "Work request opcode is not IBV_WC_SEND (%d)\n",
            wc->opcode);
    return false;
  }

  return true;
}

// Parse a RDMA receive completion
bool parse_recv_wc(struct ibv_wc *wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    fprintf(stderr, "Work request status is %s\n",
            ibv_wc_status_str(wc->status));
    return false;
  }

  if (wc->opcode != IBV_WC_RECV) {
    fprintf(stderr, "Work request opcode is not IBV_WC_RECV (%d)\n",
            wc->opcode);
    return false;
  }

  return true;
}

// Read exactly 'count' bytes from the file descriptor 'fd'
// and store the bytes into buffer 'buf'.
// Return the number of bytes successfully read.
size_t write_exact(int fd, char *buf, size_t count) {
  // current buffer loccation
  char *cur_buf = NULL;
  // # of bytes that have been written
  size_t bytes_wrt = 0;
  int n;

  if (!buf) {
    return 0;
  }

  cur_buf = buf;

  while (count > 0) {
    n = write(fd, cur_buf, count);

    if (n <= 0) {
      fprintf(stderr, "write error\n");
      break;

    } else {
      bytes_wrt += n;
      count -= n;
      cur_buf += n;
    }
  }

  return bytes_wrt;
}

// Write exactly 'count' bytes storing in buffer 'buf' into
// the file descriptor 'fd'.
// Return the number of bytes sucsessfully written.
size_t read_exact(int fd, char *buf, size_t count) {
  // current buffer loccation
  char *cur_buf = NULL;
  // # of bytes that have been read
  size_t bytes_read = 0;
  int n;

  if (!buf) {
    return 0;
  }

  cur_buf = buf;

  while (count > 0) {
    n = read(fd, cur_buf, count);

    if (n <= 0) {
      fprintf(stderr, "read error\n");
      break;

    } else {
      bytes_read += n;
      count -= n;
      cur_buf += n;
    }
  }

  return bytes_read;
}

// Build a socket connection with ip:port
int connect_socket(char *ip, uint16_t port) {
  // Create socket file descriptor
  int sockfd;
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    fprintf(stderr, "TCP socket creation error\n");
    goto err;
  }

  // Initialize server socket address
  struct sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
    fprintf(stderr, "Invalid server address %s\n", ip);
    goto err;
  }

  // Connect to the server
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    fprintf(stderr, "Fail to connect to %s:%hu\n", ip, port);
    goto err;
  }

  return sockfd;

err:
  if (sockfd >= 0) {
    close(sockfd);
  }
  return -1;
}

// Start a TCP socket server
int start_socket_server(uint16_t listen_port) {
  // Create socket file descriptor
  int sockfd = 0;
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    fprintf(stderr, "Socket creation error\n");
    goto err;
  }

  // To allow reuse of local addresses
  int opt = 1;
  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    fprintf(stderr, "Set socket option error\n");
    goto err;
  }

  struct sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(listen_port);
  if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    fprintf(stderr, "Bind error\n");
    goto err;
  }

  if (listen(sockfd, 5) < 0) {
    fprintf(stderr, "Listen error\n");
    goto err;
  }

  return sockfd;

err:
  if (sockfd >= 0) {
    close(sockfd);
  }
  return -1;
}

// Accept an connection from the client
int accept_connection(int serv_sockfd) {
  struct sockaddr_in addr;
  int addrlen = sizeof(addr);
  int cli_sockfd =
      accept(serv_sockfd, (struct sockaddr *)&addr, (socklen_t *)&addrlen);
  return cli_sockfd;
}

// Convert GUID to ethernet address
// Refer to https://docs.mellanox.com/display/MFTv4110/Setting+GUIDs+and+MACs
static void guid_to_ether_addr(uint64_t guid, char *ether_addr) {
  unsigned char out[6] = {guid >> 56, guid >> 48, guid >> 40,
                          guid >> 16, guid >> 8,  guid};
  // return *(uint64_t*)out;
  sprintf(ether_addr, "%02x:%02x:%02x:%02x:%02x:%02x", out[5], out[4], out[3],
          out[2], out[1], out[0]);
}

// Send experiment information to controller
bool notify_controller(char *ip, uint16_t port,
                       struct conn_context *connections, unsigned int num_qps) {
  const size_t max_msg_size = 20 * 2 + 64 * num_qps + 2;
  char *send_msg = NULL;
  char *recv_msg = NULL;
  char local_ether_addr[20] = {0}, remote_ether_addr[20] = {0};
  char local_gid[33] = {0};
  char remote_gid[33] = {0};

  int sockfd = connect_socket(ip, port);

  if (sockfd < 0) {
    return false;
  }

  send_msg = (char *)calloc(max_msg_size, sizeof(char));
  recv_msg = (char *)calloc(max_msg_size, sizeof(char));
  if (!send_msg || !recv_msg) {
    fprintf(stderr, "Fail to allocate send or recv message buffer\n");
    goto err;
  }

  guid_to_ether_addr(connections[0].local_dest.guid, local_ether_addr);
  guid_to_ether_addr(connections[0].rem_dest.guid, remote_ether_addr);
  sprintf(send_msg, "%s,%s", local_ether_addr, remote_ether_addr);
  sprintf(send_msg + strlen(send_msg), ";%u", num_qps);
  for (unsigned int i = 0; i < num_qps; i++) {
    struct conn_dest local_dest = connections[i].local_dest;
    struct conn_dest remote_dest = connections[i].rem_dest;
    inet_ntop(AF_INET6, &(local_dest.gid), local_gid, sizeof(local_gid));
    inet_ntop(AF_INET6, &(remote_dest.gid), remote_gid, sizeof(remote_gid));
    sprintf(send_msg + strlen(send_msg), ";%u,%u,%s;%u,%u,%s", local_dest.qpn,
            local_dest.psn, local_gid, remote_dest.qpn, remote_dest.psn,
            remote_gid);
  }
  sprintf(send_msg + strlen(send_msg), "&");

  size_t msg_len = strlen(send_msg) + 1;
  if (write_exact(sockfd, send_msg, msg_len) != msg_len) {
    fprintf(stderr, "Fail to send a %lu-byte message to the controller\n",
            msg_len);
    goto err;
  } else {
    printf("Send a %lu-byte message to the controller\n", msg_len);
  }

  if (read_exact(sockfd, recv_msg, msg_len) != msg_len ||
      strncmp(recv_msg, send_msg, msg_len) != 0) {
    fprintf(stderr, "Fail to receive the echo from the controller\n");
    goto err;
  } else {
    printf("Receive the echo from the controller\n");
  }

  free(send_msg);
  free(recv_msg);
  close(sockfd);
  return true;

err:
  free(send_msg);
  free(recv_msg);
  close(sockfd);
  return false;
}

// Validate buffer content
bool validate_buffer(struct conn_context *connections, unsigned int num_qps) {
  bool result = true;

  for (unsigned int i = 0; i < num_qps; i++) {
    for (size_t j = 0; j < connections[i].data_buf_size; j++) {
      if (connections[i].data_buf[j] != (j & 0xFF)) {
        fprintf(stderr, "Invalid data in byte %lu of QP %u\n", j, i);
        result = false;
        break;
      }
    }
  }

  return result;
}

// Exchange RDMA metadata with the server
bool exchange_metadata_with_server(int serv_sockfd,
                                   struct conn_context *connections,
                                   unsigned int num_qps, bool exchange_memory) {
  if (serv_sockfd < 0 || !connections) {
    return false;
  }

  size_t msg_size = sizeof(num_qps);
  // Exchange # of connections with the server
  if (write_exact(serv_sockfd, (char *)&num_qps, msg_size) != msg_size) {
    fprintf(stderr, "Fail to send num_qps\n");
    return false;
  }

  unsigned int serv_num_qps;
  msg_size = sizeof(serv_num_qps);
  if (read_exact(serv_sockfd, (char *)&serv_num_qps, msg_size) != msg_size) {
    fprintf(stderr, "Fail to receive num_qps from the server\n");
    return false;
  }

  if (num_qps != serv_num_qps) {
    fprintf(stderr,
            "The client and server have different numbers of QPs (%u and %u)\n",
            num_qps, serv_num_qps);
    return false;
  }

  struct conn_dest rem_dest;
  struct conn_mem rem_mem;
  size_t dest_size = sizeof(struct conn_dest);
  size_t mem_size = sizeof(struct conn_mem);

  // Exchange destination and memory with the server and connect QPs
  for (unsigned int i = 0; i < num_qps; i++) {
    if (write_exact(serv_sockfd, (char *)&(connections[i].local_dest),
                    dest_size) != dest_size) {
      fprintf(stderr, "Fail to send destination information of QP %u\n", i);
      return false;
    }

    if (read_exact(serv_sockfd, (char *)&rem_dest, dest_size) != dest_size) {
      fprintf(stderr, "Fail to receive destination information of QP %u\n", i);
      return false;
    }

    connections[i].rem_dest = rem_dest;

    if (exchange_memory) {
      if (read_exact(serv_sockfd, (char *)&rem_mem, mem_size) != mem_size) {
        fprintf(stderr, "Fail to receive memory information of QP %u\n", i);
        return false;
      } else {
        connections[i].rem_mem = rem_mem;
      }
    }

    if (!connect_qp(&connections[i], &rem_dest)) {
      fprintf(stderr, "Fail to connect QP %u to the server\n", i);
      return false;
    }

    printf("Queue pair %u\n", i);
    printf("local addr: ");
    print_dest(&(connections[i].local_dest));
    printf("remote addr: ");
    print_dest(&rem_dest);
    if (exchange_memory) {
      printf("remote memory: ");
      print_mem(&rem_mem);
    }
  }

  return true;
}

// Exchange RDMA metadata with the server
bool exchange_metadata_with_client(int cli_sockfd,
                                   struct conn_context *connections,
                                   unsigned int num_qps, bool exchange_memory)

{
  if (cli_sockfd < 0 || !connections) {
    return false;
  }

  // Exchange # of connections with the client
  unsigned int cli_num_qps;
  if (read_exact(cli_sockfd, (char *)&cli_num_qps, sizeof(cli_num_qps)) !=
      sizeof(cli_num_qps)) {
    fprintf(stderr, "Fail to receive num_qps from the client\n");
    return false;
  }

  if (write_exact(cli_sockfd, (char *)&num_qps, sizeof(num_qps)) !=
      sizeof(num_qps)) {
    fprintf(stderr, "Fail to send num_qps\n");
    return false;
  }

  if (num_qps != cli_num_qps) {
    fprintf(stderr,
            "The server and client have different numbers of QPs (%u and %u)\n",
            num_qps, cli_num_qps);
    return false;
  }

  struct conn_dest rem_dest;
  struct conn_mem local_mem;
  size_t dest_size = sizeof(struct conn_dest);
  size_t mem_size = sizeof(struct conn_mem);

  // Exchange per-connection address and memory information
  for (unsigned int i = 0; i < num_qps; i++) {
    if (read_exact(cli_sockfd, (char *)&rem_dest, dest_size) != dest_size) {
      fprintf(stderr, "Fail to read destination information of QP %u\n", i);
      return false;
    }
    connections[i].rem_dest = rem_dest;

    if (write_exact(cli_sockfd, (char *)&(connections[i].local_dest),
                    dest_size) != dest_size) {
      fprintf(stderr, "Fail to send destination information of QP %u\n", i);
      return false;
    }

    if (!connect_qp(&connections[i], &rem_dest)) {
      fprintf(stderr, "Fail to connect QP %u to the client\n", i);
      return false;
    }

    if (exchange_memory) {
      local_mem.addr = (uint64_t)(connections[i].data_mr->addr);
      local_mem.key = connections[i].data_mr->rkey;
      if (write_exact(cli_sockfd, (char *)&local_mem, mem_size) != mem_size) {
        fprintf(stderr, "Fail to send destination information of QP %u\n", i);
        return false;
      }
    }

    printf("Queue pair %u\n", i);
    printf("local addr: ");
    print_dest(&(connections[i].local_dest));
    printf("remote addr: ");
    print_dest(&rem_dest);

    if (exchange_memory) {
      printf("local memory: ");
      print_mem(&local_mem);
    }
  }

  return true;
}
