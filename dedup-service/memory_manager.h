#ifndef MEMORY_MANAGER_H
#define MEMORY_MANAGER_H

#include <boost/asio/thread_pool.hpp>
#include <boost/log/trivial.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "controller.pb.h"
#include "data_structures.h"
#include "dedup_application.h"
#include "memory_layer.h"

// Forward Declaration of DedupApplication
class DedupApplication;

class MemoryManager {
 private:
  // Post a memory request to the memory layer - returns the code returned by
  // MemoryLayer.
  int AddMemoryRequest(MemoryRequest req);

  // Complete all pending memory read requests
  void CompletePendingRequests();

  // Internal function to complete pending requests
  void _CompletePendingRequests();

  // Encode a page using Xdelta3 library
  void EncodePage(const uint8_t *input, const uint8_t *source, char *output,
                  int *output_size, int *count, std::mutex &count_mutex);

  // Decode a page using Xdelta3 library
  void DecodePage(const uint8_t *input, int input_size, const uint8_t *source,
                  char *output, int *count, std::mutex &count_mutex);

 public:
  MemoryManager(int machine_id)
      : appl_(), remaining_req_(), pending_reqs_(0), workers_(8) {
    std::unique_ptr<MemoryLayer> ml(
        new MemoryLayer("config/cluster.json", machine_id));
    ml_ = std::move(ml);

    machine_id_ = machine_id;
    patch_threshold_ = 4096;

    // Start Server for the Memory Layer, then sleep for sometime and start
    // Client too.
    ml_->start_server();
    std::this_thread::sleep_for(std::chrono::duration<double>(90));
    ml_->start_client();
    BOOST_LOG_TRIVIAL(info) << "Memory Layer is ready";
  }

  // Process the controller response, read base pages and deduplicate the dump
  void DeduplicateDump(std::shared_ptr<ContainerInfo> container,
                       std::shared_ptr<dedup::BaseContainerResponse> resp);

  // Register a single memory region in the memory dump of a container with
  // RDMA. Returns the MemoryRegionID of the registered region.
  int RegisterMemoryRegion(void *addr, size_t length) {
    return ml_->reg_src_mr(addr, length);
  }

  // Retore the image files for a deduplicated container
  void RestoreDump(std::shared_ptr<ContainerInfo> container);

  void set_no_pause(bool nopause) { no_pause_ = nopause; }

  void set_patch_threshold(int threshold) { patch_threshold_ = threshold; }

  void set_appl(std::shared_ptr<DedupApplication> appl) { appl_ = appl; }

 private:
  int machine_id_;

  // Access memory layer functions
  std::unique_ptr<MemoryLayer> ml_;

  // Hold remaining requests in a map of machine id-num pending requests.
  // Complete when max limit of pending requests reached
  std::unordered_map<int, int> remaining_req_;
  int pending_reqs_;
  std::mutex remaining_req_mutex_;

  std::weak_ptr<DedupApplication> appl_;

  // Use the thread pool for launching encoding and decoding functions
  boost::asio::thread_pool workers_;

  // System hyper-param : Threshold size of the patch in MB
  int patch_threshold_;
  bool no_pause_;
};

#endif