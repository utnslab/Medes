#include "rsrvmgr.h"

#include "rcomm.h"
#include "rparam.h"
#include "rutil.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>

static void * rsrvmgr_listen(void * context);
static inline void rsrvmgr_on_event(struct rsrvmgr * rsm, struct rdma_cm_event *event);
static inline void rsrvmgr_on_connect_request(struct rsrvmgr * rsm, struct rdma_cm_id *id);
static inline void rsrvmgr_on_connection(struct rsrvmgr * rsm, struct rdma_cm_id *id);
static inline void rsrvmgr_on_disconnect(struct rsrvmgr * rsm, struct rdma_cm_id *id);

int rsrvmgr_start(struct rsrvmgr *rsm,
                  const char *ip,
                  const int port,
                  void * callback_context,
                  void (*connect_callback)(void * context, struct rcomm_connection * conn),
                  void (*disconnect_callback)(void * context, struct rcomm_connection * conn)) {

  struct sockaddr_in addr;

  if (!rsm)  {
#ifndef NDEBUG
    fprintf(stderr, "RSRVMGR\tERROR\trsrvmgr_start\tNULL rsm\n");
#endif
    return -1;
  }

  rsm->callback_context = callback_context;
  rsm->connect_callback = connect_callback;
  rsm->disconnect_callback = disconnect_callback;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  if (ip) {
    if (!inet_pton(addr.sin_family, ip, &(addr.sin_addr))) {
#ifndef NDEBUG
      fprintf(stderr, "RSRVMGR\tERROR\trsrvmgr_start\tinet_pton failed\n");
#endif
      return -1;
    }
  } else {
    addr.sin_addr.s_addr = INADDR_ANY;
  }
  addr.sin_port = htons((uint16_t)port);

  if (!(rsm->ec = rdma_create_event_channel())) {
#ifndef NDEBUG
    fprintf(stderr, "RSRVMGR\tERROR\trsrvmgr_start\trdma_create_event_channel failed()\n");
#endif
    return -1;
  }
  
  if (rdma_create_id(rsm->ec, &rsm->listen_id, NULL, RDMA_PS_TCP)) {
#ifndef NDEBUG
    fprintf(stderr, "RSRVMGR\tERROR\trsrvmgr_start\trdma_create_id failed()\n");
#endif
    goto rsrvmgr_start_ec_created;
  }

  if (rdma_bind_addr(rsm->listen_id, (struct sockaddr *)&addr)) {
#ifndef NDEBUG
    fprintf(stderr, "RSRVMGR\tERROR\trsrvmgr_start\trdma_bind_addr failed(): %s\n", strerror(errno));
#endif
    goto rsrvmgr_start_id_created;
  }
  
  if (rdma_listen(rsm->listen_id, RSRVMGR_LISTEN_BACKLOG)) {
#ifndef NDEBUG
    fprintf(stderr, "RSRVMGR\tERROR\trsrvmgr_start\trdma_listen failed()\n");
#endif
    goto rsrvmgr_start_id_created;
  }

  if (pthread_create(&rsm->listener, NULL, &rsrvmgr_listen, (void *)rsm)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trsrvmgr_init\tpthread_create() failed\n");
#endif
    goto rsrvmgr_start_id_created;
  }

  return 0;

rsrvmgr_start_id_created:
  rdma_destroy_id(rsm->listen_id);

rsrvmgr_start_ec_created:
  rdma_destroy_event_channel(rsm->ec);

  return -1;
}


void rsrvmgr_stop(struct rsrvmgr * rsm)
{
  pthread_cancel(rsm->listener);

  rdma_destroy_id(rsm->listen_id);

  rdma_destroy_event_channel(rsm->ec);
}


void * rsrvmgr_listen(void * context) {
  struct rsrvmgr * rsm;
  struct rdma_cm_event *event;
  struct rdma_cm_event event_copy;
  
  rsm = (struct rsrvmgr *)context;

  if (set_pthread_cancel_immediately()) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trcltmgr_listen\tset_pthread_cancel_immediately failed\n");
#endif
    rsrvmgr_stop(rsm);
    pthread_exit(NULL);
  }

  while (rdma_get_cm_event(rsm->ec, &event) == 0) {

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    rsrvmgr_on_event(rsm, &event_copy);
  }

  return NULL;
}

void rsrvmgr_on_connect_request(struct rsrvmgr * rsm, struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  if (rcomm_build_connection(id)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trsrvmgr_on_connect_request\trcomm_build_connection() failed\n");
#endif

    if (rdma_reject(id, NULL, 0)) {
#ifndef NDEBUG
      fprintf(stderr, "RCLTMGR\tERROR\trsrvmgr_on_connect_request\trdma_reject() failed in handling the previous error\n");
#endif
    }

    return;
  }
  
  // build params
  memset(&cm_params, 0, sizeof(cm_params));
  cm_params.initiator_depth = cm_params.responder_resources = 1;
  cm_params.retry_count = 7;
  cm_params.rnr_retry_count = 7; /* infinite retry */

  if (rdma_accept(id, &cm_params)) {
#ifndef NDEBUG
    fprintf(stderr, "RCLTMGR\tERROR\trsrvmgr_on_connect_request\trdma_accept() failed\n");
#endif
    rcomm_destroy_connection((struct rcomm_connection *)id->context);
  }

}

void rsrvmgr_on_connection(struct rsrvmgr * rsm, struct rdma_cm_id *id)
{
  struct rcomm_connection * conn = (struct rcomm_connection *)id->context;

  rsm->connect_callback(rsm->callback_context, conn);
}

void rsrvmgr_on_disconnect(struct rsrvmgr * rsm, struct rdma_cm_id *id)
{
  struct rcomm_connection * conn = (struct rcomm_connection *)id->context;

  rsm->disconnect_callback(rsm->callback_context, conn);

  rcomm_destroy_connection(conn);  
}

void rsrvmgr_on_event(struct rsrvmgr * rsm, struct rdma_cm_event *event)
{
  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    rsrvmgr_on_connect_request(rsm, event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    rsrvmgr_on_connection(rsm, event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    rsrvmgr_on_disconnect(rsm, event->id);
  else{
#ifndef NDEBUG
    printf("RCLTMGR\tWARNING\trsrvmgr_on_event\tgot unknown event %d\n", event->event);
#endif
  }
}