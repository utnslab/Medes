#ifndef DEDUP_APPL_H
#define DEDUP_APPL_H

#include <grpcpp/grpcpp.h>

#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>

#include "client.grpc.pb.h"
#include "controller.grpc.pb.h"
#include "data_structures.h"
#include "dedup_server.h"
#include "memory_manager.h"

// Forward Declaration of DedupServerImpl
class DedupServerImpl;

// Forward Declaration of DedupServerImpl
class MemoryManager;

class DedupApplication : public std::enable_shared_from_this<DedupApplication> {
 private:
  // Send the call to the controller to register the pages of a Base Container
  // and process the response
  void RegisterBaseContainer(
      int container_id, std::shared_ptr<dedup::ContainerPages> container_pages);

  // Send the call to the controller to get the prospective base containers for
  // the deduplicated container, and proceed with deduplication
  void GetBasePages(int container_id,
                    std::shared_ptr<dedup::ContainerPages> container_pages);

  // Deduplicate a container (launched for the first time) - to get the patch
  // file corresponding to container memory state.
  void DeduplicateContainer(int container_id);

  // Write given string to CRIU pipe. Returns -1 if error occurred.
  int WriteToCRIUPipe(std::string cname);

  // Read from the CRIU Pipe and return a string of the output
  std::string ReadFromCRIUPipe(bool dedup);

  // Get the checkpoint of a container by launching the appropriate docker
  // command and communicating the dump location with the CRIU process.
  void GetContainerCheckpoint(int container_id, bool dedup);

  // Launches the Docker Force Remove Operation for the given container id
  void LaunchContainerForceRemove(int container_id);

  // Launches the Docker Purge Operation for the given container id
  void LaunchContainerPurge(int container_id);

  // Launches the Docker Pause Operation for the given container id
  void LaunchContainerPause(int container_id);

  // Launches the Docker Base Operation for the given container id
  void LaunchContainerBase(int container_id);

  // Complete the dedup operation by updating status and cleaning up redundant
  // dump files
  void FinishDeduplicate(int container_id);

  // Launches the operation to reinstate the container back to its previous
  // state before running
  void LaunchContainerPrevOp(int container_id);

  // Forward container status update rpc to controller - used for BASE and DEDUP
  // states.
  void UpdateStatusOnController(int container_id);

  // GetDecision forwards the call to the Controller machine for a particular
  // container
  void GetDecision(int container_index);

  // Compute Hashes of chunks from the pages of a container, given by cid
  // The argument flag is set `true' if memory regions are needed to be
  // registered by RDMA module else flag is `false'.
  std::shared_ptr<dedup::ContainerPages> ComputeMemoryHashes(int cid,
                                                             bool flag);

 public:
  DedupApplication(int machine_id, std::shared_ptr<grpc::Channel> channel,
                   std::size_t threads,
                   std::shared_ptr<ContainerPointerMap> container_map,
                   std::shared_ptr<DedupServerImpl> service,
                   std::shared_ptr<MemoryManager> manager)
      : machine_id_(machine_id),
        initial_mem_(0.0),
        stub_(dedup::Controller::NewStub(channel)),
        workers_(threads),
        service_(service),
        manager_(manager),
        local_containers_(),
        container_map_(container_map),
        applications_() {
    // Create FIFO Named Pipe (if not exists)
    pipe_name_ = "/tmp/criu_fifo";
    mkfifo(pipe_name_.c_str(), S_IFIFO | 0644);

    // Also create the restore fifo
    mkfifo("/tmp/restore_fifo", S_IFIFO | 0644);
  }

  // Destructor
  ~DedupApplication() {}

  // Get a shared pointer of (this)
  std::shared_ptr<DedupApplication> get_ptr() { return shared_from_this(); }

  // Get container at index
  std::shared_ptr<ContainerInfo> ContainerAtId(int cid) {
    std::shared_ptr<ContainerInfo> container_ptr;
    try {
      container_ptr = local_containers_.at(cid);
    } catch (...) {
      BOOST_LOG_TRIVIAL(info) << "Container id not available";
      return nullptr;
    }

    return container_ptr;
  }

  // Launch the dedup gRPC server. The function is blocking in nature, hence
  // should be run on a separate thread
  void RunServer(std::string address);

  // Iterates over all the containers on a machine and seeks decision when
  // needed. Acts as the Daemon which runs in the background.
  void Daemon();

  // Purge container of the given container_id
  void PurgeContainer(int container_id);

  // Restore the container of the given container_id. Called by DedupServerImpl
  // Returns 0 if successful else returns -1
  int RestoreContainer(int container_id);

  // Restore the container of the given container_id. Called by DedupServerImpl
  // Returns 0 if successful else returns -1
  // Uses the nopause method. So starts from checkpoint
  int RestoreContainerNoPause(int container_id);

  // Add a new container to the local_containers_ map. Called by DedupServerImpl
  void AddContainer(int container_id, std::string appl);

  // Update status of a container on the local_containers_ map. Called by
  // DedupServerImpl
  void UpdateContainerStatus(int container_id, ContainerStatus status);

  // Launches the Docker Dedup Operation for the given container id
  void LaunchContainerDedup(int container_id, bool dedup);

  // Launches the Docker Dedup Operation for the given container id
  void LaunchContainerDedupNoPause(int container_id, bool dedup);

  // Read the configuration as an ini file
  void ReadConfig();

  // Terminate execution
  void Terminate();

  // From /proc/meminfo and /tmp : calculate and return the total used memory.
  float CurrentUsedMemory();

 private:
  int machine_id_;

  // Hyper-parameters of the system
  int chunks_per_page_;
  int idle_time_;  // In seconds
  bool no_pause_;

  // Modifications for adaptive keep-alive policy
  bool adaptive_;

  // Track last memory usage
  float initial_mem_;
  std::chrono::time_point<std::chrono::system_clock> last_sent_;

  /* Pointer to the DedupService server that will be started when RunServer() is
   called. DedupApplication should manage the service, but the service also
   needs access to the data structres of DedupApplication. Hence, the circular
   dependency is avoided by the use of a shared_ptr-weak_ptr pair. */
  std::shared_ptr<DedupServerImpl> service_;

  // Use the thread pool for spawning functions
  boost::asio::thread_pool workers_;

  // gRPC stub for the Controller service
  std::unique_ptr<dedup::Controller::Stub> stub_;

  // List of all local containers on machine
  std::unordered_map<int, std::shared_ptr<ContainerInfo>> local_containers_;
  std::mutex container_map_mutex_;

  // Shared Pointer Map between Dedup server and agent
  std::shared_ptr<ContainerPointerMap> container_map_;

  // List of Application Environments
  std::unordered_map<std::string, std::shared_ptr<ApplConfiguration>>
      applications_;

  // Reference to the MemoryManager object responsible for managing memory dumps
  // of DEDUP/BASE containers and interfacing with RDMA
  std::shared_ptr<MemoryManager> manager_;

  // Mutex to prevent concurrent access to CRIU Named Pipe
  std::mutex restore_mutex_;
  std::mutex dump_mutex_;
  std::string pipe_name_;
};

#endif