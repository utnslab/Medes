#include "rcomm.h"
#include "rparam.h"

#include <stdio.h>
#include <stdlib.h>
#include <rdma/rdma_cma.h>

static struct rcomm_context * rcomm_build_context(struct rdma_cm_id *id);
static void rcomm_destroy_context(struct rcomm_context *ctx);

// public functions

int rcomm_build_connection(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;

  struct rcomm_context *context;
  struct rcomm_connection *conn;

  if (!id) {
#ifndef NDEBUG
    fprintf(stderr, "RCOMM\tERROR\trcomm_build_connection\tinvalid arguments\n");
#endif
    return -1;
  }

  // complete channel context
  if (!(context = rcomm_build_context(id))) {
#ifndef NDEBUG
    fprintf(stderr, "RCOMM\tERROR\trcomm_build_connection\trcomm_build_context() failed\n");
#endif
    return -1;
  }

  // create qp
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.send_cq = context->cq;
  qp_attr.recv_cq = context->cq;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = SQ_DEPTH;
  qp_attr.cap.max_recv_wr = RQ_DEPTH;
  qp_attr.cap.max_send_sge = MAX_SEND_SGE;
  qp_attr.cap.max_recv_sge = MAX_RECV_SGE;
  if (rdma_create_qp(id, context->pd, &qp_attr)) {
#ifndef NDEBUG
    fprintf(stderr, "RCOMM\tERROR\trcomm_build_connection\trdma_create_qp() failed\n");
#endif
    goto rcomm_build_connection_context_built;
  }

  // create connection context 
  conn = malloc(sizeof(struct rcomm_connection));

  // copy formerly created context objects
  conn->id = id;
  conn->qp = id->qp;
  conn->context = context;
  conn->connected = 0;
  conn->uctx = NULL;

  id->context = conn;

  return 0;

rcomm_build_connection_context_built:
  rcomm_destroy_context(context);

  return -1;
}

void rcomm_destroy_connection(struct rcomm_connection *conn)
{
  if (!conn) {
#ifndef NDEBUG
    fprintf(stderr, "RCOMM\tERROR\trcomm_destroy_connection\tinvalid arguments\n");
#endif
    return;
  }

  rcomm_destroy_context(conn->context);

  rdma_destroy_qp(conn->id);

  rdma_destroy_id(conn->id);

  free(conn);
}


// static functions

struct rcomm_context * rcomm_build_context(struct rdma_cm_id *id)
{
  struct rcomm_context *context = malloc(sizeof(*context));
  // struct ibv_cq *cq;

  context->ctx = id->verbs;

  context->pd = id->pd;

//   if (!(context->pd = ibv_alloc_pd(context->ctx))) {
// #ifndef NDEBUG
//     fprintf(stderr, "RCOMM\tERROR\trcomm_build_context\tibv_alloc_pd() failed\n");
// #endif
//     goto rcomm_build_context_failure;
//   }

  if (!(context->comp_channel = ibv_create_comp_channel(context->ctx))) {
#ifndef NDEBUG
    fprintf(stderr, "RCOMM\tERROR\trcomm_build_context\tibv_create_comp_channel() failed\n");
#endif
    goto rcomm_build_context_pd_allocated;
  }

  if (!(context->cq = ibv_create_cq(context->ctx, CQ_DEPTH, NULL, context->comp_channel, 0))) {
#ifndef NDEBUG
    fprintf(stderr, "RCOMM\tERROR\trcomm_build_context\tibv_create_cq() failed\n");
#endif
    goto rcomm_build_context_comp_channel_created;
  }

  return context;

// rcomm_build_context_cq_created:
//   ibv_destroy_cq(context->cq);

rcomm_build_context_comp_channel_created:
  ibv_destroy_comp_channel(context->comp_channel);

rcomm_build_context_pd_allocated:
  ibv_dealloc_pd(context->pd);

rcomm_build_context_failure:
  free(context);

  return NULL;
}

void rcomm_destroy_context(struct rcomm_context *ctx)
{
  ibv_destroy_cq(ctx->cq);
  ibv_destroy_comp_channel(ctx->comp_channel);
  // ibv_dealloc_pd(ctx->pd);

  free(ctx);
}
