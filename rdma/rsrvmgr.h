#ifndef RSRVMGR_H
#define RSRVMGR_H

#include "rcomm.h"

#include <rdma/rdma_cma.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C"
{
#endif

struct rsrvmgr {
  void * callback_context;
  void (*connect_callback)(void * context, struct rcomm_connection * conn);
  void (*disconnect_callback)(void * context, struct rcomm_connection * conn);

  struct rdma_event_channel *ec;
  
  struct rdma_cm_id *listen_id;
  
  pthread_t listener;
};

int rsrvmgr_start(struct rsrvmgr *rsm,
                  const char *ip,
                  const int port,
                  void * callback_context,
                  void (*connect_callback)(void * context, struct rcomm_connection * conn),
                  void (*disconnect_callback)(void * context, struct rcomm_connection * conn));

void rsrvmgr_stop(struct rsrvmgr * rsm);

#ifdef __cplusplus
}
#endif
#endif