#ifndef RCLTMGR_H
#define RCLTMGR_H

#include "rcomm.h"

#include <rdma/rdma_cma.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C"
{
#endif
/**
 * RDMA client manager (RCltMgr), active side of 
 * RDMA, responsible for establishing the 
 * connection and hand the context to flows
 */
struct rcltmgr {
  /**
   * Event channel 
   */
  struct rdma_event_channel *ec;

  /**
   * Mutex lock for connect request
   */
  pthread_mutex_t lock;

  /**
   * Listening thread for event channel
   */
  pthread_t listener;
};


/**
 * Create the client manager
 * Returns the pointer to the instance on succ
 */
int rcltmgr_init(struct rcltmgr *rcm);

/**
 * Establish connection
 * Blocking call
 * Returns pointer to context ior NULL
 */
struct rcomm_connection * 
  rcltmgr_connect(struct rcltmgr *rcm,
                  const char *ip,
                  const int port);


int rcltmgr_disconnect(
  struct rcomm_connection * conn);

void rcltmgr_uninit(struct rcltmgr * rcm); 

#ifdef __cplusplus
}
#endif

#endif