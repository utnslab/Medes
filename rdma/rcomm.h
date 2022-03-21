/**
 * Low-level communication primitives
 */
#ifndef RCOMM_H
#define RCOMM_H

#include <rdma/rdma_cma.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C"
{
#endif

struct rcomm_context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

};

/**
 * Connection context
 */ 
struct rcomm_connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  struct rcomm_context *context;

  int connected;

  void * uctx;
};

int rcomm_build_connection(struct rdma_cm_id *id);
void rcomm_destroy_connection(struct rcomm_connection *conn);

#ifdef __cplusplus
}
#endif

#endif