#include "rcltmgr.h"

#include "rcomm.h"
#include "rparam.h"
#include "rutil.h"

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <netdb.h>
#include <pthread.h>
#include <unistd.h>

static void * rcltmgr_listen(void *arg);
static inline void rcltmgr_on_event(struct rcltmgr * rcm, struct rdma_cm_event *event);
static inline void rcltmgr_on_addr_resolved(struct rcltmgr * rcm, struct rdma_cm_id *id);
static inline void rcltmgr_on_route_resolved(struct rcltmgr * rcm, struct rdma_cm_id *id);
static inline void rcltmgr_on_connection(struct rcltmgr * rcm, struct rdma_cm_id *id);
static inline void rcltmgr_on_disconnect(struct rcltmgr * rcm, struct rdma_cm_id *id);

int rcltmgr_init(struct rcltmgr *rcm)
{  
  if (!rcm) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_init\tNULL rcm\n");
#endif
    return -1;
  }

  if(!(rcm->ec = rdma_create_event_channel())) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_init\trdma_create_event_channel() failed\n");
#endif
    return -1;
  }

  if(pthread_mutex_init(&(rcm->lock), NULL)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_init\tpthread_mutex_init() failed\n");
#endif
    goto rcltmgr_init_ec_created;
  }

  // Trigger listener after all the init is donw
  if (pthread_create(&rcm->listener, NULL, &rcltmgr_listen, (void *)rcm)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_init\tpthread_create() failed\n");
#endif
    goto rcltmgr_init_mutex_inited;
  }
  
  return 0;

rcltmgr_init_mutex_inited:
  pthread_mutex_destroy(&rcm->lock);

rcltmgr_init_ec_created:
  rdma_destroy_event_channel(rcm->ec);

  return -1;
}

struct rcomm_connection * rcltmgr_connect(struct rcltmgr *rcm,
                                          const char *ip,
                                          const int port)
{
  struct addrinfo *addr;
  struct rdma_cm_id *id;
  struct rcomm_connection *conn;
  char str_port[6];

  uint64_t spin_start_time;
  int timedout;

  if (!rcm) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_connect\tNULL rcm\n");
#endif
    return NULL;
  }

  if (sprintf(str_port, "%d", port) < 0) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_connect\tinvalid port: %d\n", port);
#endif
    return NULL;
  }

  // No other connection establishment at the same time
  pthread_mutex_lock(&rcm->lock);

  // Convert address
  if (getaddrinfo(ip, str_port, NULL, &addr)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_connect\tgetaddrinfo() failed: %s:%s\n", ip, str_port);
#endif
    goto rcltmgr_connect_failure;
  }

  if (rdma_create_id(rcm->ec, &id, NULL, RDMA_PS_TCP)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_connect\trdma_create_id() failed\n");
#endif
    goto rcltmgr_connect_addrinfo_got;
  }

  if (rdma_resolve_addr(id, NULL, addr->ai_addr, RCLTMGR_RESOLVE_ADDR_TIMEOUT_MS)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_connect\trdma_resolve_addr() failed: %s:%s\n", ip, str_port);
#endif
    goto rcltmgr_connect_id_created;
  }

  // Wait for the listener to say the establishment
  timedout = 1;
  spin_start_time = gettime();
  while (gettime() < spin_start_time + RCLTMGR_CONNECT_TIMEOUT_MS * 1000000UL)
    if (id->context && __atomic_load_n(&((struct rcomm_connection *)id->context)->connected, __ATOMIC_ACQUIRE)) {
      timedout = 0;
      freeaddrinfo(addr);
      break;
    }

  conn = (struct rcomm_connection *)id->context;

  if (!timedout) {
#ifndef NDEBUG
    printf("RCLTMGR\tINFO\trcltmgr_connect\tConnected to %s:%s\n", ip, str_port);
#endif
    pthread_mutex_unlock(&rcm->lock);
    return conn;
  }

  // Not connected
#ifndef NDEBUG
  fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_connect\tConnection timed out for %s:%s\n", ip, str_port);
#endif

rcltmgr_connect_id_created:
  rdma_destroy_id(id);

rcltmgr_connect_addrinfo_got:
  freeaddrinfo(addr);

rcltmgr_connect_failure:
  pthread_mutex_unlock(&rcm->lock);
  return NULL;
}


int rcltmgr_disconnect(struct rcomm_connection * conn) {
  if (!conn) {
    return -1;
  }

  if (!__atomic_load_n(&conn->connected, __ATOMIC_ACQUIRE)) {
    rcomm_destroy_connection(conn);
    return 0;
  }

  if (rdma_disconnect(conn->id)) {
#ifndef NDEBUG
  fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_disconnect\trdma_disconnect() failed\n");
#endif
  return -1;
  }

  return 0;
}

void rcltmgr_uninit(struct rcltmgr * rcm)
{
  pthread_cancel(rcm->listener);

  pthread_mutex_destroy(&rcm->lock);

  rdma_destroy_event_channel(rcm->ec);
}


void * rcltmgr_listen(void *context)
{
  struct rcltmgr * rcm;
  struct rdma_cm_event * event;
  struct rdma_cm_event event_copy;
  
  rcm = (struct rcltmgr *)context;

  if (set_pthread_cancel_immediately()) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_listen\tset_pthread_cancel_immediately failed\n");
#endif
    rcltmgr_uninit(rcm);
    pthread_exit(NULL);
  }

  while (!rdma_get_cm_event(rcm->ec, &event)) {

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    rcltmgr_on_event(rcm, &event_copy);
  }

  return NULL;
}

/**
 * When handling RDMA_CM_EVENT_*, all errors are designed to lead to a timeout
 */
void rcltmgr_on_event(struct rcltmgr * rcm, struct rdma_cm_event *event)
{
  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    rcltmgr_on_addr_resolved(rcm, event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    rcltmgr_on_route_resolved(rcm, event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    rcltmgr_on_connection(rcm, event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    rcltmgr_on_disconnect(rcm, event->id);
  else {
#ifndef NDEBUG
    printf("RCLTMGR\tWARNING\trcltmgr_on_event\tgot unknown event %d\n", event->event);
#endif
  }

}

void rcltmgr_on_addr_resolved(struct rcltmgr * rcm, struct rdma_cm_id *id)
{
  if (rcomm_build_connection(id)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_on_addr_resolved\trcomm_build_connection() failed\n");
#endif
    return;
  }

  if (rdma_resolve_route(id, RCLTMGR_RESOLVE_ROUTE_TIMEOUT_MS)) {
#ifndef NDEBUG
    printf("RCLTMGR\tWARNING\trcltmgr_on_addr_resolved\trdma_resolve_route() failed\n");
#endif
    return;
  }
}

void rcltmgr_on_route_resolved(struct rcltmgr * rcm, struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  // build params
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.initiator_depth = cm_params.responder_resources = 1;
  cm_params.retry_count = 7;
  cm_params.rnr_retry_count = 7; /* infinite retry */

  if (rdma_connect(id, &cm_params)) {
#ifndef NDEBUG
    printf("RCLTMGR\tWARNING\trcltmgr_on_route_resolved\trdma_connect() failed\n");
#endif
    return;
  }
}

void rcltmgr_on_connection(struct rcltmgr * rcm, struct rdma_cm_id *id)
{
  struct rcomm_connection *conn = (struct rcomm_connection *)id->context;

  __atomic_store_n(&conn->connected, 1, __ATOMIC_RELEASE);
}

void rcltmgr_on_disconnect(struct rcltmgr * rcm, struct rdma_cm_id *id)
{
  struct rcomm_connection *conn = (struct rcomm_connection *)id->context;

  rcomm_destroy_connection(conn);
}
