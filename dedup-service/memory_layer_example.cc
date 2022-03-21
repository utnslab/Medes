#include "memory_layer.h"

#include <string>
#include <thread>
#include <chrono>
#include <vector>

#include <infiniband/verbs.h>

#include <unistd.h>
#include <stdio.h>

static const int MEMORY_REGION_SIZE = 4096;

int main(int argc, char *argv[]) {
  int cluster_size = std::stoi(argv[1]);
  int machine_id = std::stoi(argv[2]);
  MemoryLayer ml("config/cluster.json", machine_id);

  if (ml.start_server()) {
    EPRINTF("FATAL", "Failed to start server");
    std::exit(EXIT_FAILURE);
  }
  DPRINTF("INFO", "Server started");
  
  // sleep for 3 seconds to wait for other servers to start
  std::this_thread::sleep_for(std::chrono::duration<double>(10));
  
  int num_fail = ml.start_client();
  if (num_fail) {
    EPRINTF("ERROR", "Failed to start %d out of %d clients", num_fail, cluster_size);
    if (num_fail == cluster_size) {
      std::exit(EXIT_FAILURE);
    }
  }
  DPRINTF("INFO", "Clients connected");

  if (!fork()) {
    volatile int c;
    while (1) {
      FILE * fp = popen("cat /proc/meminfo", "r");
      while ((c = fgetc(fp)) != EOF) ;
      fclose(fp);
    }
  }

  // sleep for 3 seconds to wait for all connection to be established
  std::this_thread::sleep_for(std::chrono::duration<double>(10));

  std::vector<char *> src_bufs; // region that I want remote to read from
  std::vector<char *> dst_bufs; // region where I want the read data to be put

  for (int i=0; i<cluster_size; i++) {
    src_bufs.push_back(new char[MEMORY_REGION_SIZE]);
    dst_bufs.push_back(new char[MEMORY_REGION_SIZE]);
  }

  // Test: register memory region and broadcast to remote
  std::vector<MemoryRegionID> src_mrids;
  for (int i=0; i<cluster_size; i++) { // post asynchronous registrations.
    MemoryRegionID mrid = ml.reg_src_mr((void*)src_bufs[i], MEMORY_REGION_SIZE);
    if (mrid < 0) {
      EPRINTF("FATAL", "Failed to reg_src_mr for i=%d", i);
      std::exit(EXIT_FAILURE);
    }
    src_mrids.push_back(mrid);
  }
  DPRINTF("INFO", "All reg_src_mr succeeded");

  // Test: wait for all memory regions to be ready
  for (int i=0; i<cluster_size; i++) {
    int ret = ml.wait_src_mr(src_mrids[i], 0); 
    if (ret == 1) {
      EPRINTF("FATAL", "wait_src_mr times out for i=%d", i);
      std::exit(EXIT_FAILURE);
    }
    if (ret == -1) {
      EPRINTF("FATAL", "wait_src_mr failed for i=%d", i);
      std::exit(EXIT_FAILURE);
    }
  }
  DPRINTF("INFO", "All wait_src_mr succeeded");

  // Test: register memory region to receive read data
  std::vector<MemoryRegionID> dst_mrids;
  for (int i=0; i<cluster_size; i++) {
    MemoryRegionID mrid = ml.reg_dst_mr((void*)dst_bufs[i], MEMORY_REGION_SIZE);
    if (mrid < 0) {
      EPRINTF("FATAL", "Failed to reg_dst_mr for i=%d", i);
      std::exit(EXIT_FAILURE);
    }
    dst_mrids.push_back(mrid);
  }
  DPRINTF("INFO", "All reg_dst_mr succeeded");

  // Test reads
  for (int i=0; i<cluster_size; i++) {
    std::sprintf(src_bufs[i], "Memory content at node %d src region %d", 
      machine_id, i);
    std::sprintf(dst_bufs[i], "Untouched");
  }
  
  // sleep for 3 seconds to wait for all incoming memory reg to settle
  // and for all remote side to change content
  std::this_thread::sleep_for(std::chrono::duration<double>(10));


  // issue a read from the my_machin_id-th region of the i-th node
  for (int i=0; i<cluster_size; i++) {
    // find the my_machin_id-th region of the i-th node 
    ibv_mr * rmr = ml.lookup_raw_remote_mr(i, machine_id | MemoryRegionIDRemoteMask);
    if (!rmr) {
      EPRINTF("FATAL", "Failed to find remote mr (%d, %d | MemoryRegionIDRemoteMask)", i, machine_id);
      std::exit(EXIT_FAILURE);
    }
    // find the destination: i-th dst region
    ibv_mr * lmr = ml.lookup_raw_local_mr(i);
    if (!lmr) {
      EPRINTF("FATAL", "Failed to find local mr %d", i);
      std::exit(EXIT_FAILURE);
    }

    MemoryRequest req = {
      .context = (void*)i,
      .machine_id = i,
      .remote_addr = rmr->addr,
      .length = MEMORY_REGION_SIZE,
      .remote_mr = machine_id | MemoryRegionIDRemoteMask,
      .local_addr = dst_bufs[i],
      .local_mr = i
    };
    
    if(ml.post_request(req)) {
      EPRINTF("FATAL", "Failed to post read from %d to %d", machine_id, i);
      std::exit(EXIT_FAILURE);
    }
  }

  MemoryCompletion mcomp;
  int ret;
  for (int i=0; i<cluster_size; i++) {
    while ((ret = ml.poll_completion(i, mcomp)) == 1);
    if (ret == -1 || mcomp.status) {
      EPRINTF("FATAL", "Failed to poll from connection with node %d", i);
      std::exit(EXIT_FAILURE);
    }
  }

  DPRINTF("INFO", "All read succeeded");
  for (int i=0; i<cluster_size; i++) {
    DPRINTF("INFO", "Data from node %d:", i);
    DPRINTF("INFO", "\t%s", dst_bufs[i]);
  }

  // print some interesting information

  DPRINTF("DEBUG", "Regions that I have registered for remote read:");
  for (int i=0; i<cluster_size; i++) {
    ibv_mr * rmr = ml.lookup_raw_local_mr(i| MemoryRegionIDRemoteMask);
    
    DPRINTF("DEBUG", "--  MR (%d|RemoteMask) handle %u, starting address %p and length %lu",
      i, rmr->handle, rmr->addr, rmr->length);
  }

  DPRINTF("DEBUG", "Regions that I have received:");
  for (int i=0; i<cluster_size; i++) {
    ibv_mr * rmr = ml.lookup_raw_remote_mr(i, machine_id | MemoryRegionIDRemoteMask);
    
    DPRINTF("DEBUG", "--  MR (%d|RemoteMask) of machine %d has " \
      "handle %u, starting address %p and length %lu",
      machine_id, i, rmr->handle, rmr->addr, rmr->length);
  }

  for (auto mrid: src_mrids) {
    if (ml.dereg_mr(mrid)) {
      EPRINTF("ERROR", "Unsuccessful dereg_mr: %d|RemoteMask",  mrid & ~MemoryRegionIDRemoteMask);
    }
  }

  for (auto mrid: dst_mrids) {
    if (ml.dereg_mr(mrid)) {
      EPRINTF("ERROR", "Unsuccessful dereg_mr: %d",  mrid);
    }
  }
  DPRINTF("INFO", "All dereg_mr succeeded");

  ml.poller->join();

  return 0;
}