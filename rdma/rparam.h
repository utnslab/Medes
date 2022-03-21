#ifndef RPARAM_H
#define RPARAM_H

#define RCLTMGR_RESOLVE_ADDR_TIMEOUT_MS 200
#define RCLTMGR_RESOLVE_ROUTE_TIMEOUT_MS 200
#define RCLTMGR_CONNECT_TIMEOUT_MS 1000UL

#define RSRVMGR_LISTEN_BACKLOG 8

/**
 * Specification of QP and CQ
 * Can be set to maximum and use a soft cap
 */ 
#define SQ_DEPTH 128
#define RQ_DEPTH 128
#define CQ_DEPTH 128
#define MAX_SEND_SGE 4
#define MAX_RECV_SGE 4

#endif