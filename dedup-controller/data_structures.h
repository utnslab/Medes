// Description of the data structures used by the Dedup Controller

#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include <grpcpp/grpcpp.h>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <chrono>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "client.grpc.pb.h"
#include "defs.h"

using dedup::DedupService;
using grpc::CreateChannel;
using grpc::InsecureChannelCredentials;
using std::chrono::system_clock;

// Enum for state change policy
enum Policy {
  kDefault = 0,
  kOpenwhisk,
  kHeuristicOpenwhisk,
  kNone,
  kHeuristic,
  kBoundary
};

// Immutable class (defined by const keywords) so that no function can make
// changes to a created ChunkHashInfo object
class ChunkHashInfo {
 public:
  ChunkHashInfo(int cid, int mid, unsigned long int addr,
                unsigned long int mr_id)
      : container_id_(cid), machine_id_(mid), addr_(addr), mr_id_(mr_id) {}

  int cid() const { return container_id_; }

  int mid() const { return machine_id_; }

  unsigned long int addr() const { return addr_; }

  unsigned long int mr_id() const { return mr_id_; }

 private:
  int container_id_;
  int machine_id_;

  /* Offset to the base page in the memory region */
  unsigned long int addr_;
  /* Memory region ID of the base page, registered with RDMA */
  unsigned long int mr_id_;
};

// Class to store the information about an active Container on the machine
// Implement operations on the ContainerMap data structure here
class ContainerInfo {
 public:
  ContainerInfo(int cid, int mid, std::string appl, std::string env)
      : container_id_(cid),
        machine_id_(mid),
        appl_(appl),
        env_(env),
        status_(kDummy),
        is_base_(false),
        is_dedup_(false),
        next_assigned_(false),
        blacklisted_(false),
        num_failed_(0),
        refcount_(0) {
    idle_state_ = std::chrono::system_clock::now();
    last_modified_ = std::chrono::system_clock::now();
  }

  int machine_id() { return machine_id_; }

  int refcount() { return refcount_; }

  bool is_base() { return is_base_; }

  bool is_dedup() { return is_dedup_; }

  bool next_assigned() { return next_assigned_; }

  bool blacklisted() { return blacklisted_; }

  int num_failed() { return num_failed_; }

  std::chrono::time_point<std::chrono::system_clock> idle_state() {
    return idle_state_;
  }

  void ResetIdleState() { idle_state_ = std::chrono::system_clock::now(); }

  void IncrementNumFailed() { num_failed_ = num_failed_ + 1; }

  ContainerStatus status() { return status_; }

  std::string env() { return env_; }

  std::string appl() { return appl_; }

  std::chrono::time_point<std::chrono::system_clock> last_modified() {
    return last_modified_;
  }

  void set_next_assigned(bool flag) { next_assigned_ = flag; }

  void set_blacklisted() { blacklisted_ = true; }

  void UpdateStatus(ContainerStatus status) {
    status_ = status;
    if (status == kBase) {
      is_base_ = true;
    }
    if (status == kDedup) {
      is_dedup_ = true;
    }
    if (status != kDummy) {
      // Dummy state is used as an ephemeral state => Does not signify a state
      // change. Hence, not counted to check since when a container has been
      // in a "valid" state.
      last_modified_ = std::chrono::system_clock::now();
    }
  }

  void IncrementRefcount() { refcount_++; }

 private:
  int container_id_;
  int machine_id_;
  int refcount_;
  int num_failed_;
  bool is_base_;
  bool is_dedup_;
  bool next_assigned_;
  bool blacklisted_;
  std::string env_;
  std::string appl_;
  ContainerStatus status_;
  std::chrono::time_point<std::chrono::system_clock> idle_state_;
  std::chrono::time_point<std::chrono::system_clock> last_modified_;
};

// Use a shared DataStructures class for all objects running on the controller.
// NOTE: The write/update methods in the data structures of this class must be
// thread-safe as they can be invoked by concurrent workers.
class DataStructures {
 private:
  // Class to store Machine Information. Currently, uses only memory available
  // for load. Also encapsulates the stub for contacting the server on the
  // respective machine.
  class MachineInfo {
   public:
    MachineInfo(int mid, std::string target)
        : machine_id_(mid), num_dedup_starts_(0) {
      total_memory_ = 0;
      used_memory_ = 0;
      auto channel =
          grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
      stub_ = DedupService::NewStub(channel);
    }

    void set_total_memory(int init_mem) { total_memory_ = init_mem; }

    void set_used_memory(float used_mem) { used_memory_ = used_mem; }

    float memory() { return (total_memory_ - used_memory_); }

    bool has_enough_memory() { return used_memory_ < 0.95 * total_memory_; }

    int num_dedup_starts() { return num_dedup_starts_; }

    void IncrementDedupStarts() { num_dedup_starts_ = num_dedup_starts_ + 1; }

    void DecrementDedupStarts() { num_dedup_starts_ = num_dedup_starts_ - 1; }

    int machine_id_;
    int num_dedup_starts_;
    std::unique_ptr<DedupService::Stub> stub_;
    float used_memory_;
    float total_memory_;
  };

  // Class to hold stats and information required for policy implementation.
  // Currently uses number of warm containers, number of dedup containers and
  // request arrival rates.
  class EnvironmentInfo {
   public:
    EnvironmentInfo(int memory, double dedup_benefit)
        : num_warm_(0),
          num_base_(0),
          num_dedup_(0),
          memory_(memory),
          dedup_benefit_(dedup_benefit),
          warm_start_times_(0.0),
          dedup_start_times_(0.0),
          alpha_(0.1),
          requests_in_last_minute_(),
          rates_(),
          rates_mutex_() {}

    // Stats
    int num_warm_;
    int num_base_;
    int num_dedup_;

    // Accumulators to keep track of warm and dedup startup times
    // NOTE: Keeps track of exponentially moving average
    double warm_start_times_;
    double dedup_start_times_;

    // Weights of recent startup times shall be more (hence, keeping the load
    // into account)
    double alpha_;

    // Memory occupied by the environment container
    double memory_;
    double dedup_benefit_;

    int requests_in_last_minute_;
    std::vector<std::pair<int, system_clock::time_point>> rates_;
    boost::shared_mutex rates_mutex_;
  };

  // Class to hold information required for a particular application [separately
  // configurable].
  class ApplicationInfo {
   public:
    ApplicationInfo(int kw, int et, Policy policy)
        : policy_(policy), keep_warm_(kw), exec_time_(et) {}

    void set_policy(Policy policy) { policy_ = policy; }

    // Policy information
    int keep_warm_;
    int exec_time_;
    Policy policy_;
  };

 public:
  // Ctor
  DataStructures()
      : global_cid_(0),
        machine_map_(),
        round_robin_(0),
        keep_warm_(10 /* In minutes */),
        keep_dedup_(10 /* In minutes */),
        exec_time_(1000 /* In milliseconds */),
        slo_(2200 /* Just smaller than cold start latency */) {}

  int reuse_period() { return reuse_period_; }

  void set_reuse_period(int time) { reuse_period_ = time; }

  void set_idle_period(int time) { idle_period_ = time; }

  void set_request_window(int window) { request_window_ = window; }

  void set_default_policy(Policy policy) { default_policy_ = policy; }

  // Get container info for the given cid. The semantics of thread-safe
  // operation should be ensured at the calling functions.
  std::shared_ptr<ContainerInfo> GetContainerInfo(int cid) {
    boost::shared_lock<boost::shared_mutex> lock(container_map_mutex_);
    return container_map_.at(cid);
  }

  // Get machine info for communication to a specific machine id
  std::shared_ptr<MachineInfo> GetMachineInfo(int machine_id) {
    return machine_map_.at(machine_id);
  }

  std::shared_ptr<EnvironmentInfo> GetEnvironmentStats(std::string env) {
    return policy_stats_.at(env);
  }

  std::shared_ptr<ApplicationInfo> GetApplicationInfo(std::string appl) {
    return appl_info_.at(appl);
  }

  // Add a machine into the machine map
  void AddMachineInfo(int machine_id, std::string target, int memory) {
    auto minfo_ptr = std::make_shared<MachineInfo>(machine_id, target);
    minfo_ptr->set_total_memory(memory);
    machine_map_.push_back(std::move(minfo_ptr));
  }

  // Return the total number of containers on the platform
  int GetTotalContainers() { return container_map_.size(); }

  // Get the memory fraction that must be used by an environment
  double GetEnvMemoryFraction(std::string env);

  // Get Machine with maximum available memory
  int GetMaxMemoryMachine();

  // Get Machine in the round robin fashion
  int RoundRobinMachine();

  // Check and get the cid of an existing container to run a function with the
  // given environment. Returns -1 if no container is available to run.
  int GetAvailableContainer(std::string appl, std::string exec_env,
                            double time_since_arrival);

  // Get the most suitable environment whose containers can be evicted.
  std::string GetEvictionEnvironment();

  // Given the execution environment, iterate over all containers and choose the
  // least recently used for eviction candidate.
  int GetEvictionCandidate(std::string exec_env);

  // Update container status for container with the given cid
  void UpdateContainerStatus(std::shared_ptr<ContainerInfo> c_info,
                             ContainerStatus status);

  // Add a chunk hash on the hash table
  void AddChunkHash(std::string hash, std::shared_ptr<ChunkHashInfo> chunk);

  // Get the vector of entries corresponding to a particular hash entry
  std::vector<std::shared_ptr<ChunkHashInfo>> FindHash(std::string hash);

  // Add a new container to the container map and return the cid of the new
  // container
  int AddNewContainer(int machine_id, std::string appl, std::string env);

  // Remove a container because it has been purged
  void RemoveContainer(int container_id);

  // Get the arrival rate of the current window - unit is requests per second
  double GetMovingWindowArrivalRate(std::string env);

  // Get the maximum arrival rate of the past window - requests per second
  double GetMaxArrivalRate(std::string env);

  // Update the arrival rate for a particular environment
  void UpdateArrivalRates(
      std::string env,
      std::chrono::time_point<std::chrono::system_clock> curr_time);

  // Update startup times for the respective environment
  void UpdateStartupTimes(std::string env, double start_time, bool is_warm);

  // Update policy stat information for the given container, whose previous
  // state is given by the argument. purge set true if a container is purged.
  void UpdateEnvStats(std::shared_ptr<ContainerInfo> c_info,
                      ContainerStatus prev_status, bool purge);

  // Add a new application to store information
  void AddNewApplication(std::string appl, int keep_warm, int exec_time) {
    appl_info_.insert(
        make_pair(appl, std::make_shared<ApplicationInfo>(keep_warm, exec_time,
                                                          default_policy_)));
  }

  // Add a new application to track stats
  void AddNewEnvironment(std::string env, double memory, double dedup_benefit) {
    policy_stats_.insert(make_pair(
        env, std::make_shared<EnvironmentInfo>(memory, dedup_benefit)));
  }

 private:
  // Current container id (for new containers)
  int global_cid_;

  // Data structure to hold all containers on the cluster
  std::unordered_map<int, std::shared_ptr<ContainerInfo>> container_map_;
  boost::shared_mutex container_map_mutex_;

  // Data structure to manage machines
  std::vector<std::shared_ptr<MachineInfo>> machine_map_;
  int round_robin_;
  std::mutex round_robin_mutex_;

  // Default policy in case no policy set for an application
  // NOTE: Currently unused.
  int keep_warm_;   // in minutes
  int keep_dedup_;  // in minutes
  int exec_time_;   // in ms
  int slo_;         // in ms (time to complete execution)
  Policy default_policy_;

  // Hyper parameters to the system
  int idle_period_;     // in ms
  int reuse_period_;    // in ms
  int request_window_;  // Number of minutes to take in the window

  // Data structure to store information for each application
  std::unordered_map<std::string, std::shared_ptr<ApplicationInfo>> appl_info_;

  // Data structure to store policy stats per environment
  std::unordered_map<std::string, std::shared_ptr<EnvironmentInfo>>
      policy_stats_;

  // Data structure to register and read chunk hashes
  std::unordered_map<std::string, std::vector<std::shared_ptr<ChunkHashInfo>>>
      hash_table_;
  boost::shared_mutex hash_table_mutex_;
};

#endif
