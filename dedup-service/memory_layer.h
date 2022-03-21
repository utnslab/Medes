#ifndef MEMORY_LAYER_H
#define MEMORY_LAYER_H

#include <cstdint>

#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include "rcltmgr.h"
#include "rsrvmgr.h"

// ---------------- BEGIN debug utility ----------------
#include <cstdio>

#ifndef NDEBUG
#define DPRINTF(LEVEL, FORMAT, ...)                                       \
  {                                                                       \
    std::printf("%s\t%s\t" FORMAT "\n", LEVEL,                     \
                __func__, ##__VA_ARGS__);  \
    std::fflush(stdout);                                                  \
  }
#else
#define DPRINTF(...) { }
#endif

#define EPRINTF(LEVEL, FORMAT, ...)                                       \
  {                                                                       \
    std::fprintf(stderr, "%s\t%s\t" FORMAT "\n", LEVEL,            \
                __func__, ##__VA_ARGS__);  \
    std::fflush(stderr);                                                  \
  }

// ---------------- END debug utility ----------------

using MachineID = std::int32_t;
using MemoryRegionID = std::int32_t;

// Most significant 4 bits are used for metadata
static const std::int32_t MemoryRegionIDMaxValue = 1 << 28 - 1; 
static const std::int32_t MemoryRegionIDRemoteMask = 1 << 28; 

static const int MAX_NUM_PENDING_MRS = 64; // min(sq_length/2, rq_length)
static const int MAX_NUM_PENDING_REQS = 64; // sq_length - MAX_NUM_PENDING_MRS

// ------------------ BEGIN Public Structs ------------------
// The memory request posted to MemoryLayer for work
struct MemoryRequest {
  // User context that will be copied to the MemoryCompletion to identify
  // what request/context the completion object belongs to
  void * context;

  // ID of the machine to post this request to 
  int machine_id;

  // Read [remote_addr, remote_addr+length) to local_addr
  void * remote_addr;
  std::uint32_t length;
  MemoryRegionID remote_mr;
  void * local_addr;
  MemoryRegionID local_mr;
};

struct MemoryCompletion {
  static const std::int32_t STATUS_SUCCESS = 0;
  static const std::int32_t STATUS_FAILURE = 1;

  std::int32_t status;
  void * context;
};

// ------------------ END Public Structs ------------------

class SrcMemoryRegion {
  friend class InboundConnection;
  friend class MemoryLayer;

  MemoryRegionID mr_id;
  ibv_mr * mr;

  std::size_t target_success_cnt;
  std::size_t success_cnt;

  std::mutex mutex;
  std::condition_variable cond;

  void * context;
  void (*callback)(void* context);
};

class DstMemoryRegion {
  friend class MemoryNode;
  friend class MemoryLayer;

  MemoryRegionID mr_id;
  ibv_mr * mr;
};

class MRMessaging {
protected:
  rcomm_connection* conn_;

  struct MRMessage {
    MachineID machine_id;
    MemoryRegionID mr_id;
    ibv_mr mr;
    SrcMemoryRegion * smr;
  };
  
  MRMessage send_msg[MAX_NUM_PENDING_MRS];
  ibv_mr * send_msg_mr;
  struct SendContext {
    ibv_send_wr wr;
    ibv_sge sge;
    int is_complete;
  } send_ctxs[MAX_NUM_PENDING_MRS];
  std::size_t send_ctx_head;
  std::size_t send_ctx_tail;

  MRMessage recv_msg[MAX_NUM_PENDING_MRS];
  ibv_mr * recv_msg_mr;
  struct RecvContext {
    ibv_recv_wr wr;
    ibv_sge sge;
    int is_complete;
  } recv_ctxs[MAX_NUM_PENDING_MRS];
  std::size_t recv_ctx_head;
  std::size_t recv_ctx_tail;

  std::mutex msg_mutex;

  int init(rcomm_connection * conn);

  // -------- BEGIN Predicate: must posess msg_mutex && has empty slot --------
  int post_head_recv();

  MRMessage* get_head_send_msg();
  int post_head_send();
  // -------- END Predicate: must posess msg_mutex && has empty slot --------
};

class InboundConnection: public MRMessaging {
  friend class MemoryLayer;

  MachineID machine_id_;

  void poll();

  // Predicate: has empty slot
  // Returns 0 on successfully posting the send
  // Returns -1 on failure
  int send_mr(std::shared_ptr<SrcMemoryRegion> smr);

  // not implemented
  // should handle the case where we failed to send this mr
  int recall_mr(std::shared_ptr<SrcMemoryRegion> smr);
  
public: // only to be used by make_shared
  InboundConnection(MachineID machine_id, rcomm_connection* conn);
};

class MemoryNode: public MRMessaging {
  friend class MemoryLayer;

  MachineID machine_id_;
  std::string addr_;
  int port_;

  struct ReqContext {
    ibv_send_wr wr;
    ibv_sge sge;
    int is_complete;
    void * uctx;
  } req_ctxs[MAX_NUM_PENDING_REQS];
  std::size_t req_ctx_head;
  std::size_t req_ctx_tail;
  std::mutex req_mutex;

  std::queue<MemoryCompletion> comp_queue_;
  std::mutex comp_queue_mutex_;

  void poll();

  // Predicate: must possess req_mutex  && has empty slot
  int post_head_req();

  // Stores all MRs received from this remote node for access from local machine 
  std::unordered_map<MemoryRegionID, ibv_mr> src_mr_map_;
  std::mutex src_mr_mutex_;

  int connect(rcltmgr* rcm);

  // not implemented
  int disconnect(rcltmgr* rcm);

  // get raw MR object
  ibv_mr * lookup_raw_remote_mr(MemoryRegionID mrid);

  // posts one memory request. Returns 0 on success, -1 on failure.
  // Asynchronous call
  int post_request(const MemoryRequest & mreq, ibv_mr * lmr);
  
  // Polls one memory completion event. 
  // Returns 0 on success, 1 on not found, -1 on failure.
  // Asynchronous call
  int poll_completion(MemoryCompletion & mcomp);

public: // only to be used by make_shared
  MemoryNode(MachineID machine_id, const std::string & addr, int port);
};


class MemoryLayer {
  friend int main(int, char**);
  
  MachineID machine_id_;

  int is_configured_;

  std::shared_ptr<MemoryNode> my_node;

  // rsrvmgrs use a unique PD for all server-created RDMA objects and thus we 
  // only need to register a region once. pd is to make sure that all inbound 
  // connections belong to the same PD
  // A different pdid being generated probably means connections come from
  // multiple devices, which is a case we have not considered
  rsrvmgr rsm_;
  ibv_pd pd; 
  std::unordered_map<rcomm_connection*, std::shared_ptr<InboundConnection>> 
    conns_map_;
  std::mutex conns_mutex_;

  std::unordered_map<MemoryRegionID, std::shared_ptr<SrcMemoryRegion>> 
    src_mrs_map_;
  int src_mrs_cnt_; // always increment
  std::mutex src_mrs_mutex_;
  std::atomic<unsigned int> pending_mrs_cnt_;
  static void src_mr_ready_cb_(void* context);

  rcltmgr rcm_;
  ibv_pd clt_pd; 
  // assuming that nodes don't change
  // if nodes are to change, use a LOCK!
  std::unordered_map<MachineID, std::shared_ptr<MemoryNode>> node_map_;


  std::unordered_map<MemoryRegionID, std::shared_ptr<DstMemoryRegion>> 
    dst_mrs_map_;
  int dst_mrs_cnt_;
  std::mutex dst_mrs_mutex_;

  std::shared_ptr<std::thread> poller;

  // TODO: add lock if need dynamic changes to node_map
  std::shared_ptr<MemoryNode> get_memory_node(MachineID machine_id);

  static void poll(MemoryLayer * ml);

public:
  // Accepts the filename of the config JSON and the id of this machine
  MemoryLayer(std::string config_filename, MachineID machine_id);
  
  // Whether the initialization in constructor is successful
  int is_configured();

  // Starts listening to connections
  // Binds to address specified by the config file
  // Returne 0 on success, -1 on failure
  int start_server();
  static void on_connect(void * context, rcomm_connection * conn);
  static void on_disconnect(void * context, rcomm_connection * conn);

  // Try connecting to all clients as per config file
  // Returns 0 on success for all or the negative number of failures
  int start_client();

  // Register the given address and length for remote side to read from
  // and start the broadcast to all others
  // On success return value >= 0
  // On failure of registering at local side returns -1
  // On failure of initiating entire broadcast returns -2, 
  //  the system can be left in inconsistent view about this MR
  MemoryRegionID reg_src_mr(void * addr, std::size_t length);

  // Returns 1 for true
  // Returns 0 for false
  // Returns -1 for failure
  int is_src_mr_ready(MemoryRegionID mr_id);
  
  // Wait for a source MemoryRegion to be globally ready
  // Returns 0 on success
  // Returns 1 if timeout_us expires
  // Returns -1 for other failures
  int wait_src_mr(MemoryRegionID mr_id, int timeout_us = 0);

  // Register the given address and length as destination of locally issued read
  // Blocking call
  // On success return value >= 0
  // On failure returns -1
  MemoryRegionID reg_dst_mr(void * addr, std::size_t length);

  // Returns a pointer to the raw RDMA MR object that is locally registered
  // Returns nullptr on failure
  ibv_mr * lookup_raw_local_mr(MemoryRegionID mrid);

  // Returns a pointer to the raw RDMA MR object of a remote machine if the MR
  //  has been received from the MR broadcast mechanism.
  // Returns nullptr on failure
  ibv_mr * lookup_raw_remote_mr(MachineID machine_id, MemoryRegionID mrid);

  // not implemented: Release the memory region
  // Remote view is not guaranteed to be handled 
  // On success returns 0; on MR not found returns 1; on other failures -1
  int dereg_mr(MemoryRegionID mrid);

  // posts one memory request. Returns 0 on success, -1 on failure.
  // Asynchronous call
  // mreq can be released once this function returns 
  int post_request(const MemoryRequest & mreq);
  
  // Polls one memory completion event. 
  // Returns 0 on success, 1 on nothing polled, -1 on error.
  int poll_completion(MachineID machine_id, MemoryCompletion & mcomp);

};

#endif
