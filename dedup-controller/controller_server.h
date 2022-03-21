// Server for the DedupController service

#ifndef CONTROLLER_SERVER_H
#define CONTROLLER_SERVER_H

#include <grpcpp/grpcpp.h>

#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <mutex>

#include "controller.grpc.pb.h"
#include "data_structures.h"
#include "main.grpc.pb.h"

using dedup::Ack;
using dedup::BaseContainerResponse;
using dedup::Container;
using dedup::ContainerPages;
using dedup::Controller;
using dedup::DecisionResponse;
using dedup::MemoryArgs;
using dedup::StatusArgs;
using grpc::ServerContext;
using grpc::Status;

class ControllerServerImpl final : public Controller::Service {
 private:
  // Schedule the requests on appropriate container (WARM, DEDUP or COLD)
  class Scheduler {
   public:
    Scheduler(std::shared_ptr<DataStructures> ds)
        : ds_(ds),
          dropped_reqs_(0),
          dedup_starts_per_machine_(10),
          num_failures_(5) {}

    void ScheduleRequest(std::string appl, std::string exec_env);

    void EvictContainer(std::string exec_env);

    int dropped_reqs_;
    std::mutex dropped_mutex_;

    int completed_reqs_;
    std::mutex completed_mutex_;

    // Number of dedup containers that can be started together per machine
    int dedup_starts_per_machine_;

    // Number of failures before the container is purged
    int num_failures_;

    std::shared_ptr<DataStructures> ds_;

    std::mutex scheduler_mutex_;
  };

  // Various policies

  DecisionResponse::Decision DedupNoneOpenwhisk(int cid, std::string env);

  DecisionResponse::Decision DedupNoneHeuristic(int cid, std::string appl,
                                                std::string env);

  DecisionResponse::Decision DedupHeuristic(int cid, std::string appl,
                                            std::string env);

  DecisionResponse::Decision DedupBoundaryHeuristic(int cid, std::string appl,
                                                    std::string env);

  DecisionResponse::Decision DedupHeuristicOpenwhisk(int cid, std::string appl,
                                                     std::string env);

 public:
  ControllerServerImpl(std::size_t threads)
      : workers_(threads),
        num_machines_(0),
        base_ready_(false),
        scheduler_(new Scheduler(data_structures_)),
        data_structures_(new DataStructures()) {}

  Status GetDecision(ServerContext* ctxt, const Container* args,
                     DecisionResponse* resp);

  // RPC called by a BASE container
  Status RegisterPages(ServerContext* ctxt, const ContainerPages* args,
                       Ack* ack);

  // RPC called by a DEDUP container
  Status GetBaseContainers(ServerContext* ctxt, const ContainerPages* args,
                           BaseContainerResponse* resp);

  // RPC called to notify the successful change of Status of a container
  Status UpdateStatus(ServerContext* ctxt, const StatusArgs* args, Ack* ack);

  // RPC called to update memory usage for each machine
  Status UpdateAvailableMemory(ServerContext* ctxt, const MemoryArgs* args,
                               Ack* ack);

  // RPC called to update a blacklisted contaienr
  Status Blacklist(ServerContext* ctxt, const Container* args, Ack* ack);

  // Read (from some stream - cin or ifstream) and serve user requests
  void ServeRequests(std::string filename);

  // Read configuration from controller ini file (using INIReader)
  void ReadConfig();

  // Setup connections to the gRPC servers at Dedup Agents
  void SetupConnections();

  // Spawn a single container
  void SpawnSingleContainer(int mid);

  // OBSOLETE: Always use atleast 80% of the total cluster memory
  void SpawnContainers();

 private:
  // A shared reference to the data structures stored at the controller. Used by
  // other member objects at the controller.
  std::shared_ptr<DataStructures> data_structures_;

  // An instance of the scheduler class, which abstracts the scheduling logic
  // from the remainder of the controller logic.
  std::unique_ptr<Scheduler> scheduler_;

  // Use the thread pool for spawning functions
  boost::asio::thread_pool workers_;

  // Heuristics set at experiment run time
  // Weights for choosing base containers
  std::vector<double> base_choice_weights_;

  // Weights for state policy
  std::vector<double> state_policy_weights_;

  // Number of machines
  int num_machines_;
  float mem_cap_;

  // Set if some base containers have been registered
  bool base_ready_;

  // Hyper-parameters
  // Number of dedup containers per base container - Used in Policy
  int dedup_per_base_;

  // Heuristic parameter
  double alpha_;
  double beta_;
  double gamma_;

  // What objective to minimize
  bool latency_constraint_;

  // Heuristic threshold above which containers get deduplicated
  double threshold_;

  // Calculation of provisioned containers in heuristic
  int provisioned_;
};

#endif