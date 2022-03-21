#include "memory_layer.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#include <infiniband/verbs.h>

#include <chrono>

#include <cerrno>
#include <cstring>

int MRMessaging::init(rcomm_connection * conn) {
  conn_ = conn;

  std::memset((void*)send_msg, 0, MAX_NUM_PENDING_MRS * sizeof(MRMessage));
  std::memset((void*)send_ctxs, 0, MAX_NUM_PENDING_MRS * sizeof(*send_ctxs));
  send_ctx_head = send_ctx_tail = 0UL;
  std::memset((void*)recv_msg, 0, MAX_NUM_PENDING_MRS * sizeof(MRMessage));
  std::memset((void*)recv_ctxs, 0, MAX_NUM_PENDING_MRS * sizeof(*recv_ctxs));
  recv_ctx_head = recv_ctx_tail = 0UL;

  send_msg_mr = ibv_reg_mr(conn->context->pd, (void*)send_msg,
    MAX_NUM_PENDING_MRS * sizeof(MRMessage),
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!send_msg_mr) {
    EPRINTF("ERROR", "Failed register MR for sending message");
    return -1;
  }

  recv_msg_mr = ibv_reg_mr(conn->context->pd, (void*)recv_msg,
    MAX_NUM_PENDING_MRS * sizeof(MRMessage),
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!recv_msg_mr) {
    EPRINTF("ERROR", "Failed register MR for sending message");
    ibv_dereg_mr(send_msg_mr); // function call should be atomic
    return -1;
  }

  for (int i=0; i<MAX_NUM_PENDING_MRS; i++) {
    // Preset fields for send
    send_ctxs[i].wr.wr_id = (std::uint64_t)i;
    send_ctxs[i].wr.sg_list = &send_ctxs[i].sge;
    send_ctxs[i].wr.num_sge = 1;
    send_ctxs[i].wr.opcode = IBV_WR_SEND;
    send_ctxs[i].wr.send_flags = IBV_SEND_SIGNALED;

    send_ctxs[i].sge.addr = (uintptr_t)&send_msg[i];
    send_ctxs[i].sge.length = sizeof(MRMessage);
    send_ctxs[i].sge.lkey = send_msg_mr->lkey;

    // Preset fields for recv
    recv_ctxs[i].wr.wr_id = (std::uint64_t)i;
    recv_ctxs[i].wr.sg_list = &recv_ctxs[i].sge;
    recv_ctxs[i].wr.num_sge = 1;
  
    recv_ctxs[i].sge.addr = (uintptr_t)&recv_msg[i];
    recv_ctxs[i].sge.length = sizeof(MRMessage);
    recv_ctxs[i].sge.lkey = recv_msg_mr->lkey;
  }  
  return 0;
}

int MRMessaging::post_head_recv() {
  int slot, ret;
  ibv_recv_wr* bad_recv_wr;

  slot = recv_ctx_head % MAX_NUM_PENDING_MRS;
  ret = ibv_post_recv(conn_->qp, &recv_ctxs[slot].wr, &bad_recv_wr);
  recv_ctxs[slot].is_complete = 0;
  if (ret) {
    EPRINTF("ERROR", "Failed to ibv_post_recv: %s", std::strerror(ret));
    return -1;
  }
  recv_ctx_head++;

  return ret;
}

MRMessaging::MRMessage* MRMessaging::get_head_send_msg() {
  return &send_msg[send_ctx_head % MAX_NUM_PENDING_MRS];
}

int MRMessaging::post_head_send() {
  int slot, ret;
  ibv_send_wr* bad_send_wr;

  slot = send_ctx_head % MAX_NUM_PENDING_MRS;
  ret = ibv_post_send(conn_->qp, &send_ctxs[slot].wr, &bad_send_wr);
  send_ctxs[slot].is_complete = 0;
  if (ret) {
    EPRINTF("ERROR", "Failed to ibv_post_send: %s", std::strerror(ret));
    return -1;
  }
  send_ctx_head++;
  return ret;
}

// ---------------- BEGIN MemoryNode ----------------

MemoryNode::MemoryNode(MachineID machine_id, 
                       const std::string & addr,
                       int port): 
  machine_id_(machine_id), addr_(addr), port_(port) {

}

int MemoryNode::connect(rcltmgr* rcm) { 
  if (!(conn_ = rcltmgr_connect(rcm, addr_.c_str(), port_))) {
    EPRINTF("ERROR", "Failed to connect to %d at %s:%d", 
            machine_id_, addr_.c_str(), port_);
    return -1;
  }

  // initialize MRMessaging state
  if (init(conn_)) {
    EPRINTF("ERROR", "Failed to initialized MR messaging state");
    return -1;
  }
  
  // Post to receive incoming MRs
  {
    std::lock_guard<std::mutex> msg_lock(msg_mutex);
    for (int i=0; i<MAX_NUM_PENDING_MRS; i++) {
      if(post_head_recv()) {
        EPRINTF("ERROR", "Failed to post initial recvs");
        return -1;
      }
    }
  }

  std::memset(req_ctxs, 0, MAX_NUM_PENDING_REQS*sizeof(*req_ctxs));
  req_ctx_head = req_ctx_tail = 0;

  for (int i=0; i<MAX_NUM_PENDING_REQS; i++) {
    // Preset fields for send
    req_ctxs[i].wr.wr_id = (std::uint64_t)i;
    req_ctxs[i].wr.sg_list = &req_ctxs[i].sge;
    req_ctxs[i].wr.num_sge = 1;
    req_ctxs[i].wr.opcode = IBV_WR_RDMA_READ;
    req_ctxs[i].wr.send_flags = IBV_SEND_SIGNALED;
  }

  return 0; 
}

int MemoryNode::disconnect(rcltmgr* rcm) { 
  return 0;
}

int MemoryNode::post_head_req() {
  int slot, ret;
  ibv_send_wr* bad_wr;

  slot = req_ctx_head % MAX_NUM_PENDING_MRS;
  ret = ibv_post_send(conn_->qp, &req_ctxs[slot].wr, &bad_wr);
  req_ctxs[slot].is_complete = 0;
  if (ret) {
    EPRINTF("ERROR", "Failed to ibv_post_send: %s", std::strerror(ret));
    return -1;
  }
  req_ctx_head++;
  return ret;
}

ibv_mr * MemoryNode::lookup_raw_remote_mr(MemoryRegionID mrid) {
  std::lock_guard<std::mutex> src_mr_lock(src_mr_mutex_);
  auto iter = src_mr_map_.find(mrid);
  if (iter == src_mr_map_.end()) {
    return nullptr;
  }
  return &iter->second;
}

int MemoryNode::post_request(const MemoryRequest & mreq, ibv_mr * lmr) { 
  // lookup for remote mr
  ibv_mr* rmr = lookup_raw_remote_mr(mreq.remote_mr);
  if (!rmr) {
    EPRINTF("ERROR", "Failed to get raw mr for remote_mr %d", mreq.remote_mr);
    return -1;
  }
  
  {
    std::lock_guard<std::mutex> req_lock(req_mutex);
    if (req_ctx_head >= req_ctx_tail + MAX_NUM_PENDING_REQS) {
      return -1;
    }

    int slot = req_ctx_head % MAX_NUM_PENDING_REQS;
    req_ctxs[slot].wr.wr.rdma.remote_addr = (uintptr_t)mreq.remote_addr;
    req_ctxs[slot].wr.wr.rdma.rkey = rmr->rkey;
    req_ctxs[slot].sge.addr = (uintptr_t)mreq.local_addr;
    req_ctxs[slot].sge.length = mreq.length;
    req_ctxs[slot].sge.lkey = lmr->lkey;
    req_ctxs[slot].uctx = mreq.context;

    if (post_head_req()) {
      return -1;
    }
  }

  return 0;
}

int MemoryNode::poll_completion(MemoryCompletion & mcomp) { 
  std::lock_guard<std::mutex> comp_queue_lock(comp_queue_mutex_);
  if (comp_queue_.empty()) {
    return 1;
  } 
  mcomp = comp_queue_.front();
  comp_queue_.pop();
  return 0;
}

void MemoryNode::poll() {
  ibv_wc wc;
  int slot;
  std::unique_lock<std::mutex> msg_lock(msg_mutex,  std::defer_lock);
  std::unique_lock<std::mutex> src_mr_lock(src_mr_mutex_, std::defer_lock);
  std::unique_lock<std::mutex> req_lock(req_mutex, std::defer_lock);
  std::unique_lock<std::mutex> comp_queue_lock(comp_queue_mutex_, std::defer_lock);

  while (ibv_poll_cq(conn_->context->cq, 1, &wc) > 0) {
    if (wc.opcode == IBV_WC_SEND) {
      if (wc.status != IBV_WC_SUCCESS) {
        EPRINTF("ERROR", "A send completed with status %d", wc.status);
      }

      msg_lock.lock();
      slot = (int)wc.wr_id;
      send_ctxs[slot].is_complete = 1;
      msg_lock.unlock();
    
    } else if (wc.opcode == IBV_WC_RECV) {
      if (wc.status != IBV_WC_SUCCESS) {
        EPRINTF("ERROR", "A recv completed with status %d", wc.status);
      }

      // begin:msg_lock
      msg_lock.lock();
      slot = (int)wc.wr_id;
      MemoryRegionID mrid = recv_msg[slot].mr_id;
      ibv_mr mr;
      recv_ctxs[slot].is_complete = 1;
      std::memcpy((void *)&mr, (void *)&recv_msg[slot].mr, sizeof(ibv_mr));
      msg_lock.unlock();

      src_mr_lock.lock();
      src_mr_map_.insert({recv_msg[slot].mr_id, recv_msg[slot].mr});
      src_mr_lock.unlock();
      
      // reply with ack
      msg_lock.lock();
      MRMessage * send_msg = get_head_send_msg();
      std::memcpy((void*)send_msg, (void*)&recv_msg[slot], sizeof(MRMessage));
      post_head_send();
      msg_lock.unlock();
      DPRINTF("INFO", "Received a new MR from machine %d w/ len %lu; ack posted", 
              machine_id_, mr.length);
      // end:msg_lock

    } else if (wc.opcode == IBV_WC_RDMA_READ) {
      MemoryCompletion mc;
      if (wc.status != IBV_WC_SUCCESS) {
        EPRINTF("ERROR", "A read completed with status %d", wc.status);
        mc.status = MemoryCompletion::STATUS_FAILURE;
      } else {
        mc.status = MemoryCompletion::STATUS_SUCCESS;
      }

      req_lock.lock();
      slot = (int)wc.wr_id;
      req_ctxs[slot].is_complete = 1;
      mc.context = req_ctxs[slot].uctx;
      req_lock.unlock();

      comp_queue_lock.lock();
      comp_queue_.push(std::move(mc));
      comp_queue_lock.unlock();
      
    }
  }


  // begin:msg_lock
  msg_lock.lock();
  while (send_ctxs[send_ctx_tail % MAX_NUM_PENDING_MRS].is_complete 
      && send_ctx_tail < send_ctx_head) {
    send_ctxs[send_ctx_tail % MAX_NUM_PENDING_MRS].is_complete = 0;
    send_ctx_tail++;
  }

  while (recv_ctxs[recv_ctx_tail % MAX_NUM_PENDING_MRS].is_complete 
      && recv_ctx_tail < recv_ctx_head) {
    recv_ctxs[recv_ctx_tail % MAX_NUM_PENDING_MRS].is_complete = 0;
    recv_ctx_tail++;
    post_head_recv();
  }
  msg_lock.unlock();

  req_lock.lock();
  while (req_ctxs[req_ctx_tail % MAX_NUM_PENDING_REQS].is_complete 
      && req_ctx_tail < req_ctx_head) {
    req_ctxs[req_ctx_tail % MAX_NUM_PENDING_REQS].is_complete = 0;
    req_ctx_tail++;
  }
  req_lock.unlock();
}

// ---------------- END MemoryNode ----------------


// ---------------- BEGIN InboundConnection ----------------

InboundConnection::InboundConnection( MachineID machine_id,
  rcomm_connection* conn): machine_id_(machine_id) {

  if (init(conn)) {
    EPRINTF("ERROR", "Failed to initialized MR messaging state");
  }
}

int InboundConnection::send_mr(std::shared_ptr<SrcMemoryRegion> smr) { 
  std::lock_guard<std::mutex> msg_lock(msg_mutex);
    
  // Predicate guarantees that there has to be empty slot

  MRMessage* send_head_msg = get_head_send_msg();
  send_head_msg->machine_id = machine_id_;
  send_head_msg->mr_id = smr->mr_id;
  send_head_msg->smr = smr.get();
  std::memcpy((void*)&send_head_msg->mr, (void*)smr->mr, sizeof(ibv_mr));

  if (post_head_send()) {
    return -1;
  }

  if (post_head_recv()) {
    return -1;
  }

  return 0;
}

int InboundConnection::recall_mr(std::shared_ptr<SrcMemoryRegion> smr) {
  return 0;
}


void InboundConnection::poll() {
  ibv_wc wc;
  int slot;

  std::unique_lock<std::mutex> msg_lock(msg_mutex);
  slot = (recv_ctx_head == recv_ctx_tail);
  msg_lock.unlock();
  
  if (slot) {
    return;
  }

  while (ibv_poll_cq(conn_->context->cq, 1, &wc) > 0) {
    if (wc.opcode == IBV_WC_SEND) {
      if (wc.status != IBV_WC_SUCCESS) {
        EPRINTF("ERROR", "A send completed with status %d", wc.status);
      }
      slot = (int)wc.wr_id;
      
      msg_lock.lock();
      send_ctxs[slot].is_complete = 1;
      msg_lock.unlock();
    } else if (wc.opcode == IBV_WC_RECV) {
      if (wc.status != IBV_WC_SUCCESS) {
        EPRINTF("ERROR", "A recv completed with status %d", wc.status);
      }
      slot = (int)wc.wr_id;

      msg_lock.lock();
      recv_ctxs[slot].is_complete = 1;

      SrcMemoryRegion* smr = recv_msg[slot].smr;
      msg_lock.unlock();

      std::unique_lock<std::mutex> smr_lock(smr->mutex);
      smr->success_cnt++;
      if (smr->success_cnt == smr->target_success_cnt) {
        DPRINTF("INFO", "Ack of source MR %d received", smr->mr_id);
        smr->cond.notify_all();
        smr->callback(smr->context);
      }
      smr_lock.unlock();
    }
  }

  msg_lock.lock();
  while (send_ctxs[send_ctx_tail % MAX_NUM_PENDING_MRS].is_complete 
      && send_ctx_tail < send_ctx_head) {
    send_ctxs[send_ctx_tail % MAX_NUM_PENDING_MRS].is_complete = 0;
    send_ctx_tail++;
  }

  while (recv_ctxs[recv_ctx_tail % MAX_NUM_PENDING_MRS].is_complete 
      && recv_ctx_tail < recv_ctx_head) {
    recv_ctxs[recv_ctx_tail % MAX_NUM_PENDING_MRS].is_complete = 0;
    recv_ctx_tail++;
  }
  msg_lock.unlock();
}

// ---------------- END InboundConnection ----------------


MemoryLayer::MemoryLayer(std::string config_filename, MachineID machine_id):
  machine_id_(machine_id), is_configured_(0), src_mrs_cnt_(0), 
  pending_mrs_cnt_(0), dst_mrs_cnt_(0) {

  ibv_fork_init();
  try {
    DPRINTF("INFO", "Configuring");

    boost::property_tree::ptree tree;
    boost::property_tree::read_json(config_filename, tree);
    
    BOOST_FOREACH(boost::property_tree::ptree::value_type &v, 
                  tree.get_child("memory_nodes")) { 

      int mid = v.second.get<int>("machine_id");
      std::string addr = v.second.get<std::string>("addr");
      int port = v.second.get<int>("port");

      node_map_.insert({
        mid,
        std::make_shared<MemoryNode>(mid, addr, port)
      });

      DPRINTF("INFO", "\tMemory node %d at %s:%d", mid, addr.c_str(), port);
    }

    DPRINTF("INFO", "\tMy machine_id is %d", machine_id);

  } catch (const std::exception & e) {
    EPRINTF("ERROR", "Failed to open %s: %s", config_filename.c_str(), e.what());
    return;
  }

  auto my_node_iter = node_map_.find(machine_id_);
  if (my_node_iter == node_map_.end()) {
    EPRINTF("ERROR", "My machine_id is not found in memory node list.");
    return;
  }
  my_node = my_node_iter->second;

  is_configured_ = 1;

  pd.context = 0;
  clt_pd.context = 0;
}

int MemoryLayer::is_configured() {
  return is_configured_;
}

int MemoryLayer::start_server() {
  if (!is_configured()) {
    EPRINTF("ERROR", "Not configured");
    return -1;
  }

  if (rsrvmgr_start(&rsm_, my_node->addr_.c_str(), my_node->port_, 
                     (void*)this, on_connect, on_disconnect)) {
    
    EPRINTF("ERROR", "Failed to start RDMA server");
    return -1;
  }
  DPRINTF("INFO",  "RDMA server listening to %d", my_node->port_);
  
  return 0;
}

void MemoryLayer::on_connect(void * context, rcomm_connection * conn) {
  MemoryLayer* ml = (MemoryLayer*)context;
  
  { // Critical section: conns_map_
    std::lock_guard<std::mutex> conns_lock(ml->conns_mutex_);
    auto element = ml->conns_map_.find(conn);
    if (element != ml->conns_map_.end()) {
      EPRINTF("WARNING", "The same connection already exists: %p; " \
        "ignoring the new connection", (void*)conn); 
      return;
    }
    
    if (!ml->pd.context) {
      ml->pd.context = conn->context->pd->context;
      ml->pd.handle = conn->context->pd->handle;
    } else if (conn->context->pd->handle != ml->pd.handle) {
      EPRINTF("WARNING", "A different protection domain handle is seen. "    \
                          "All memory region registered later will belong to "\
                        "the new PD."); 
      ml->pd.context = conn->context->pd->context;
      ml->pd.handle = conn->context->pd->handle;
    } else if (conn->context->pd->context != ml->pd.context) {
      EPRINTF("WARNING", "A different ibv_context in PD is seen but the "  \
                          "handle is the same, so there's probably nothing "\
                          "wrong. Using the new ibv_context."); 
      ml->pd.context = conn->context->pd->context;
    }
    
    ml->conns_map_.insert({conn, std::make_shared<InboundConnection>(ml->machine_id_, conn)});
  }
}

void MemoryLayer::on_disconnect(void * context, rcomm_connection * conn) {
  MemoryLayer* ml = (MemoryLayer*)context;
  
  { // Critical section: conns_map_
    std::lock_guard<std::mutex> conns_lock(ml->conns_mutex_);
    auto element = ml->conns_map_.find(conn);
    if (element == ml->conns_map_.end()) {
      EPRINTF("WARNING", "The connection is not found: %p", (void*)conn); 
      return;
    }
    ml->conns_map_.erase(conn);
  }
}

int MemoryLayer::start_client() {
  if (rcltmgr_init(&rcm_)) {
    EPRINTF("ERROR", "Failed to initialize RDMA client");
    return -1;
  }

    // need lock if nodes can be added dynamically
  for (auto node_map_kv : node_map_) {
    const auto & machine_id = node_map_kv.first;
    const auto & node = node_map_kv.second;

    if (node->connect(&rcm_)) {
      EPRINTF("ERROR", "Failed to connect to machine %d", machine_id);
      continue;
    }

    if (!clt_pd.context){
      clt_pd.context = node->conn_->context->pd->context;
      clt_pd.handle = node->conn_->context->pd->handle;
    } else if (node->conn_->context->pd->handle != clt_pd.handle) {
      if (clt_pd.context){
        EPRINTF("WARNING", "A different protection domain handle is seen. "      \
                            "All dst memory region registered later will belong "\
                            "to the new PD."); 
      }
      clt_pd.context = node->conn_->context->pd->context;
      clt_pd.handle = node->conn_->context->pd->handle;
    } else if (node->conn_->context->pd->context != clt_pd.context) {
      EPRINTF("WARNING", "A different ibv_context in PD is seen but the "  \
                          "handle is the same, so there's probably nothing "\
                          "wrong. Using the new ibv_context."); 
      clt_pd.context = node->conn_->context->pd->context;
    }
  }

  try {
    poller = std::make_shared<std::thread>(poll, this);
  } catch (const std::exception & e) {
    EPRINTF("ERROR", "Failed to start the poller thread: %s", e.what());
    return -1;
  }

  return 0;
}

void MemoryLayer::src_mr_ready_cb_(void* context) {
  MemoryLayer * ml = (MemoryLayer*)context;
  ml->pending_mrs_cnt_--;
}

MemoryRegionID MemoryLayer::reg_src_mr(void * addr, std::size_t length) {
  if (pending_mrs_cnt_ >= MAX_NUM_PENDING_MRS) {
    EPRINTF("ERROR", "Num of pending MRs has reached maximum");
    return -1;
  }

  ibv_mr * mr = ibv_reg_mr(&pd, addr, length, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

  if (!mr) {
    EPRINTF("ERROR", "ibv_reg_mr failed: %s", std::strerror(errno));
    return -1;
  }

  std::shared_ptr<SrcMemoryRegion> smr = std::make_shared<SrcMemoryRegion>();
  smr->mr = mr;
  smr->success_cnt = 0;
  smr->context = (void*) this;
  smr->callback = src_mr_ready_cb_;
  // insert src_mr to map
  { // Critical section: src_mrs_map_
    std::lock_guard<std::mutex> src_mrs_lock(src_mrs_mutex_);
    if (src_mrs_cnt_ >= MemoryRegionIDMaxValue) {
      EPRINTF("ERROR", "Source memory region ID has been used up");
      ibv_dereg_mr(mr);
      return -1;
    }
    smr->mr_id = src_mrs_cnt_ | MemoryRegionIDRemoteMask;
    src_mrs_map_.insert({smr->mr_id, smr});
    src_mrs_cnt_++;
  }

  // submit for broadcast
  std::size_t target_send_success_cnt;
  std::size_t send_success_cnt = 0;
  { // Critical section: conns_map_
    std::lock_guard<std::mutex> conns_lock(conns_mutex_);
    target_send_success_cnt = conns_map_.size();
    for (const auto & elem : conns_map_) {
      send_success_cnt += (!elem.second->send_mr(smr));
    }
    smr->target_success_cnt = send_success_cnt;
  }
  if (send_success_cnt) {
    pending_mrs_cnt_++;
  }

  if (send_success_cnt < target_send_success_cnt) {
    EPRINTF("ERROR", "Source MR %d (len %lu) only initiated broadcast to %lu "
      "out of %lu nodes", smr->mr_id, length, 
      send_success_cnt, target_send_success_cnt);
    return -2;
  }

  DPRINTF("INFO", "Source MR %d (len %lu) broadcast successfully posted", 
    smr->mr_id, length);

  return smr->mr_id;
}

int MemoryLayer::is_src_mr_ready(MemoryRegionID mr_id) {
  std::shared_ptr<SrcMemoryRegion> smr;

  { // Critical section: src_mrs_map_
    std::lock_guard<std::mutex> src_mrs_lock(src_mrs_mutex_);
    auto iter = src_mrs_map_.find(mr_id);
    if (iter == src_mrs_map_.end()) {
      EPRINTF("Warning", "MemoryRegionID %d not found", mr_id);
      return -1;
    }
    smr = iter->second;
  }

  { // Critical section: smr
    std::lock_guard<std::mutex> smr_lock(smr->mutex);
    return (smr->success_cnt == smr->target_success_cnt);
  }
}

int MemoryLayer::wait_src_mr(MemoryRegionID mr_id, int timeout_us) {
  std::shared_ptr<SrcMemoryRegion> smr;

  { // Critical section: src_mrs_map_
    std::lock_guard<std::mutex> src_mrs_lock(src_mrs_mutex_);
    auto iter = src_mrs_map_.find(mr_id);
    if (iter == src_mrs_map_.end()) {
      EPRINTF("Warning", "MemoryRegionID %d not found", mr_id);
      return -1;
    }
    smr = iter->second;
  }

  // Critical section: smr
  std::unique_lock<std::mutex> smr_lock(smr->mutex);
  if (!timeout_us) {
    smr->cond.wait(smr_lock, 
      [&smr]{return smr->success_cnt == smr->target_success_cnt;});
  } else {
    if(!smr->cond.wait_for(smr_lock, 
        std::chrono::microseconds(timeout_us), 
        [&smr]{return smr->success_cnt == smr->target_success_cnt;})) { 
      // failed to get the lock
      return 1;
    }
  }
  smr_lock.unlock();

  return 0;
}

MemoryRegionID MemoryLayer::reg_dst_mr(void * addr, std::size_t length) {
  ibv_mr * mr = ibv_reg_mr(&clt_pd, addr, length, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
  
  if (!mr) {
    // EPRINTF("ERROR", "Failed to register destination MR");
    EPRINTF("ERROR", "Failed to register destination MR: %s", std::strerror(errno));
    return -1;
  }

  std::shared_ptr<DstMemoryRegion> dmr = std::make_shared<DstMemoryRegion>();
  dmr->mr = mr;
  {
    std::lock_guard<std::mutex> dst_mrs_lock(dst_mrs_mutex_);
    if (dst_mrs_cnt_ >= MemoryRegionIDMaxValue) {
      EPRINTF("ERROR", "Destination memory region ID has been used up");
      ibv_dereg_mr(mr);
      return -1;
    }
    dmr->mr_id = dst_mrs_cnt_;
    dst_mrs_map_.insert({dmr->mr_id, dmr});
    dst_mrs_cnt_++;
  }

  return dmr->mr_id;
}

ibv_mr * MemoryLayer::lookup_raw_local_mr(MemoryRegionID mrid) {
  ibv_mr * mr = nullptr;
  if (mrid & MemoryRegionIDRemoteMask) { // src
    std::lock_guard<std::mutex> src_mrs_lock(src_mrs_mutex_);
    auto iter = src_mrs_map_.find(mrid);
    if (iter == src_mrs_map_.end()) {
      return nullptr;
    }
    mr = iter->second->mr;
  } else { // dst
    std::lock_guard<std::mutex> dst_mrs_lock(dst_mrs_mutex_);
    auto iter = dst_mrs_map_.find(mrid);
    if (iter == dst_mrs_map_.end()) {
      return nullptr;
    }
    mr = iter->second->mr;
  }
  return mr;
}

ibv_mr * MemoryLayer::lookup_raw_remote_mr(MachineID machine_id, MemoryRegionID mrid) {
  std::shared_ptr<MemoryNode> mnode = get_memory_node(machine_id);
  if (!mnode) {
    return nullptr;
  }

  return mnode->lookup_raw_remote_mr(mrid);
}

int MemoryLayer::dereg_mr(MemoryRegionID mr_id) {
  if (mr_id & MemoryRegionIDRemoteMask) { // src
    std::lock_guard<std::mutex> src_lock(src_mrs_mutex_);

    auto iter = src_mrs_map_.find(mr_id);
    if (iter == src_mrs_map_.end()) {
      return 1;
    }

    int res = ibv_dereg_mr(iter->second->mr);
    if (res) {
      EPRINTF("ERROR", "ibv_dereg_mr failed for (%d|RemoteMask)",
        mr_id & ~MemoryRegionIDRemoteMask);
      return -1;
    }

    src_mrs_map_.erase(iter);
  } else { // dst
    std::lock_guard<std::mutex> dst_lock(dst_mrs_mutex_);

    auto iter = dst_mrs_map_.find(mr_id);
    if (iter == dst_mrs_map_.end()) {
      return 1;
    }

    int res = ibv_dereg_mr(iter->second->mr);
    if (res) {
      EPRINTF("ERROR", "ibv_dereg_mr failed for %d", mr_id);
      return -1;
    }
    
    dst_mrs_map_.erase(iter);
  }

  return 0;
}

int MemoryLayer::post_request(const MemoryRequest & mreq) { 
  std::shared_ptr<MemoryNode> mnode = get_memory_node(mreq.machine_id);
  if (!mnode) {
    EPRINTF("ERROR", "Machine ID %d not found", mreq.machine_id);
    return -1;
  }

  ibv_mr * lmr = lookup_raw_local_mr(mreq.local_mr);
  if (!lmr) {
    EPRINTF("ERROR", "Destnation MR ID %d not found", mreq.local_mr);
    return -1;
  }

  return mnode->post_request(mreq, lmr);
}

int MemoryLayer::poll_completion(MachineID machine_id, MemoryCompletion & mcomp) { 
  std::shared_ptr<MemoryNode> mnode = get_memory_node(machine_id);
  if (!mnode) {
    EPRINTF("ERROR", "Machine ID %d not found", machine_id);
    return -1;
  }

  return mnode->poll_completion(mcomp);
}

std::shared_ptr<MemoryNode> MemoryLayer::get_memory_node(MachineID machine_id) {
  auto iter = node_map_.find(machine_id);
  if (iter == node_map_.end()) {
    EPRINTF("ERROR", "Machine ID %d not found", machine_id);
    return std::shared_ptr<MemoryNode>();
  }
  return iter->second;
}

void MemoryLayer::poll(MemoryLayer * ml) {
  std::unique_lock<std::mutex> conns_lock(ml->conns_mutex_,std::defer_lock);
  while (1) {
    if (ml->pending_mrs_cnt_) {
      conns_lock.lock();
      for (auto iter=ml->conns_map_.begin(); iter!=ml->conns_map_.end(); iter++) {
        iter->second->poll();
      }
      conns_lock.unlock();
    }

    // need lock if nodes can be added dynamically
    for (auto iter=ml->node_map_.begin(); iter!=ml->node_map_.end(); iter++) {
      iter->second->poll();
    }
  }
}