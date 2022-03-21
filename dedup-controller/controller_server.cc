// Server for the DedupController service

#include "controller_server.h"

#include <grpcpp/grpcpp.h>

#include <boost/asio/post.hpp>
#include <boost/foreach.hpp>
#include <boost/log/trivial.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>
#include <vector>

#include "INIReader.h"
#include "controller.grpc.pb.h"
#include "main.grpc.pb.h"

using boost::asio::post;
using dedup::Ack;
using dedup::BaseContainerResponse;
using dedup::Container;
using dedup::ContainerPages;
using dedup::Controller;
using dedup::DecisionResponse;
using dedup::MemoryResponse;
using dedup::SpawnArgs;
using dedup::StatusArgs;
using dedup::StatusArgs_Status;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using std::getline;
using std::ifstream;
using std::istringstream;
using std::make_pair;
using std::make_shared;
using std::map;
using std::pair;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unordered_map;
using std::vector;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::system_clock;
using std::chrono::time_point;

using Decision = dedup::DecisionResponse_Decision;
using PageStruct = dedup::ContainerPages_HashStruct;
using BasePage = dedup::BaseContainerResponse_BasePage;

DecisionResponse::Decision ControllerServerImpl::DedupNoneOpenwhisk(
    int cid, string env) {
  // Dedup Agent seeks decision after ten minute period - and hence, the
  // decision is to purge.
  return DecisionResponse::PURGE;
}

DecisionResponse::Decision ControllerServerImpl::DedupNoneHeuristic(
    int cid, string appl, string env) {
  auto stats = data_structures_->GetEnvironmentStats(env);
  auto appl_info = data_structures_->GetApplicationInfo(appl);

  // OBSOLETE (POLICY 2) := [Alpha, Beta] thresholds
  // int requirement = stats->arrival_rate_ *
  //                   (stats->exec_time_ + data_structures_->reuse_period());
  // BOOST_LOG_TRIVIAL(info) << "Requirement " << requirement << " Available "
  //                         << num_available;
  // double threshold = data_structures_->GetMaxArrivalRate(env);

  double reuse = appl_info->exec_time_ + data_structures_->reuse_period();
  double warm_reuse_period = (stats->warm_start_times_ + reuse) / 1000;

  double provisioned_req =
      ((double)(stats->num_warm_ + stats->num_base_) / warm_reuse_period);

  // For requirement - use maximum and moving average rates
  double upper_threshold = data_structures_->GetMaxArrivalRate(env);

  if (provisioned_req > upper_threshold) {
    return DecisionResponse::PURGE;
  }
  return DecisionResponse::WARM;
}

DecisionResponse::Decision ControllerServerImpl::DedupHeuristic(int cid,
                                                                string appl,
                                                                string env) {
  // Heuristic policy
  auto stats = data_structures_->GetEnvironmentStats(env);
  auto appl_info = data_structures_->GetApplicationInfo(appl);
  Decision tentative_decision;

  // OBSOLETE (POLICY 1) := Lambda * KW / 2
  // if (stats->num_warm_ >= stats->arrival_rate_ * stats->keep_warm_ / 2) {
  //   return DecisionResponse::DEDUP;
  // }

  // OBSOLETE (POLICY 2) := [Alpha, Beta] thresholds
  // Use the requests vector directly instead of the arrival rate
  // int requirement = stats->arrival_rate_ *
  //                   (stats->exec_time_ + data_structures_->reuse_period());
  // int num_available = stats->num_warm_ + stats->num_dedup_;
  // BOOST_LOG_TRIVIAL(info) << "Requirement " << requirement << " Available "
  //                         << num_available << " Exp Req " << expected_req;

  // Use the start-up times directly (In seconds)
  double reuse = appl_info->exec_time_ + data_structures_->reuse_period();
  double warm_reuse_period = (stats->warm_start_times_ + reuse) / 1000;
  double dedup_reuse_period = (stats->dedup_start_times_ + reuse) / 1000;

  double provisioned_req = 0.0;
  if (provisioned_ == 0) {
    // Use only warm containers
    provisioned_req =
        ((double)(stats->num_warm_ + stats->num_base_) / warm_reuse_period);
  } else {
    // Use warm+dedup containers
    provisioned_req =
        ((double)(stats->num_warm_ + stats->num_base_) / warm_reuse_period) +
        ((double)stats->num_dedup_ / dedup_reuse_period);
  }

  // For requirement - use maximum and moving average rates
  double upper_threshold = data_structures_->GetMaxArrivalRate(env);
  double lower_threshold = data_structures_->GetMovingWindowArrivalRate(env);

  BOOST_LOG_TRIVIAL(info) << "Provisioned " << provisioned_req << " LT "
                          << lower_threshold << " UT " << upper_threshold;

  auto c_info = data_structures_->GetContainerInfo(cid);
  auto m_info = data_structures_->GetMachineInfo(c_info->machine_id());

  // Purge only if memory on machine is limited
  if (provisioned_req > gamma_ * upper_threshold) {
    tentative_decision = DecisionResponse::PURGE;
  } else if (provisioned_req > lower_threshold && base_ready_) {
    tentative_decision = DecisionResponse::DEDUP;
  } else {
    tentative_decision = DecisionResponse::WARM;
  }

  // Check if it is already a base container
  if (c_info->is_base()) {
    return DecisionResponse::BASE;
  }

  // Check if more BASE containers needed, and container is not dedup
  if (stats->num_dedup_ >= stats->num_base_ * dedup_per_base_) {
    if (!c_info->is_dedup()) {
      stats->num_base_++;
      return DecisionResponse::BASE;
    }
  }

  // Check if tentative decision is feasible := DEDUP to WARM not possible
  if (c_info->is_dedup() && tentative_decision == DecisionResponse::WARM) {
    return DecisionResponse::DEDUP;
  }

  return tentative_decision;
}

DecisionResponse::Decision ControllerServerImpl::DedupBoundaryHeuristic(
    int cid, string appl, string env) {
  // Heuristic policy
  auto stats = data_structures_->GetEnvironmentStats(env);
  auto appl_info = data_structures_->GetApplicationInfo(appl);
  Decision tentative_decision = DecisionResponse::WARM;

  // Use the start-up times directly (In seconds)
  double reuse = appl_info->exec_time_ + data_structures_->reuse_period();
  double warm_reuse_period = (stats->warm_start_times_ + reuse) / 1000;
  double dedup_start_time =
      stats->dedup_start_times_ > 0.0001 ? stats->dedup_start_times_ : 300;
  double dedup_reuse_period = (dedup_start_time + reuse) / 1000;

  auto c_info = data_structures_->GetContainerInfo(cid);
  auto m_info = data_structures_->GetMachineInfo(c_info->machine_id());

  // Purge as per the keep alive policy
  auto curr_time = system_clock::now();
  auto time_in_idle = duration_cast<duration<double, std::milli>>(
                          curr_time - c_info->idle_state())
                          .count();

  if (time_in_idle >= 600 * 1000) {
    if (c_info->status() == kWarm) {
      BOOST_LOG_TRIVIAL(info) << "Purge warm container";
      tentative_decision = DecisionResponse::PURGE;
    } else if (c_info->status() == kDedup) {
      if (time_in_idle >= 900 * 1000) {
        BOOST_LOG_TRIVIAL(info) << "Purge dedup container " << time_in_idle;
        tentative_decision = DecisionResponse::PURGE;
      }
    }
  } else {
    // Dedup so as to minimize objective, given certain constraints.
    // Useful expressions
    int total_containers =
        stats->num_warm_ + stats->num_base_ + stats->num_dedup_;
    double dedup_cost = warm_reuse_period / dedup_reuse_period;

    // First need to check if the load constraint has a valid solution.
    double d_lambda =
        (total_containers -
         data_structures_->GetMaxArrivalRate(env) * warm_reuse_period) /
        (1 - dedup_cost);

    BOOST_LOG_TRIVIAL(trace)
        << "Total containers: " << total_containers << " "
        << " arr rate: " << data_structures_->GetMaxArrivalRate(env)
        << " cost: " << dedup_cost;

    // Now, make decision depending on whether we want to minimize the memory
    // usage or the latency bound.
    if (latency_constraint_) {
      if (d_lambda < 0) {
        // Even if all current containers are warm, we cannot fulfill the
        // requests
        BOOST_LOG_TRIVIAL(trace)
            << "Load cannot be fulfilled " << d_lambda << " "
            << data_structures_->GetMaxArrivalRate(env);
        tentative_decision = DecisionResponse::WARM;
      } else {
        double latency_threshold = 2.5;
        double frac =
            (dedup_start_time * dedup_cost / stats->warm_start_times_) - 1;

        BOOST_LOG_TRIVIAL(info) << "Frac " << frac << " cost " << dedup_cost
                                << " WRP " << warm_reuse_period;

        double d_latency = (latency_threshold * warm_reuse_period *
                                data_structures_->GetMaxArrivalRate(env) -
                            total_containers) /
                           frac;

        double d_opt = d_latency < d_lambda ? d_latency : d_lambda;

        BOOST_LOG_TRIVIAL(info) << "Optimal " << d_opt << " latency "
                                << d_latency << " lambda " << d_lambda;

        if (d_latency < 0) {
          // NOTE: This implies that even if we keep all dedup containers then
          // also we can meet the load constraints.
          tentative_decision = DecisionResponse::PURGE;
        } else if (stats->num_dedup_ < d_opt) {
          tentative_decision = DecisionResponse::DEDUP;
        } else {
          tentative_decision = DecisionResponse::WARM;
        }
      }
    } else {
      // Get the memory fraction that can be allotted to this application
      double memory_fraction = data_structures_->GetEnvMemoryFraction(env);
      // double d_memory =
      //     (total_containers - (memory_fraction / stats->memory_)) /
      //     (stats->dedup_benefit_ - ((double)15 / stats->memory_));

      BOOST_LOG_TRIVIAL(info) << "Stats mem " << stats->memory_ << " Bene "
                              << stats->dedup_benefit_;

      double d_memory =
          (total_containers - (memory_fraction / stats->memory_)) /
          stats->dedup_benefit_;

      BOOST_LOG_TRIVIAL(info)
          << "Required " << d_memory << " provisioned " << stats->num_dedup_
          << " mem " << memory_fraction;

      if (d_memory < 0) {
        // Enough memory available -- Keep warm
        tentative_decision = DecisionResponse::WARM;
      } else if (d_memory > total_containers) {
        // NOTE: This implies we have too many containers. Hence, must be purged
        // to meet the memory bounds. DEDUP gives too much memory benefits
        tentative_decision = DecisionResponse::DEDUP;
        // if (d_lambda > total_containers) {
        //   tentative_decision = DecisionResponse::PURGE;
        // } else {
        //   tentative_decision = DecisionResponse::DEDUP;
        // }
      } else {
        if (d_memory < d_lambda) {
          // Feasible solution to the constraints existing - policy will try to
          // converge to the solution.
          if (stats->num_dedup_ < d_memory) {
            tentative_decision = DecisionResponse::DEDUP;
          } else {
            tentative_decision = DecisionResponse::WARM;
          }
        } else {
          // No feasible solution to the problem found -- implies the current
          // memory usage is not sufficient to serve the incoming load, but we
          // need to keep the containers around, therefore DEDUP.
          tentative_decision = DecisionResponse::DEDUP;
        }
      }
    }
  }

  // Check if it is already a base container
  if (c_info->is_base()) {
    return DecisionResponse::BASE;
  }

  // Check if more BASE containers needed, and container is not dedup
  if (stats->num_dedup_ >= stats->num_base_ * dedup_per_base_) {
    if (!c_info->is_dedup()) {
      stats->num_base_++;
      return DecisionResponse::BASE;
    }
  }

  // if (stats->num_dedup_ == 0) {
  //   return DecisionResponse::DEDUP;
  // }

  // Check if tentative decision is feasible := DEDUP to WARM not possible
  if (c_info->is_dedup() && tentative_decision == DecisionResponse::WARM) {
    return DecisionResponse::DEDUP;
  }

  // Check if the container is blacklisted - Keep Warm instead of BASE or DEDUP
  if (c_info->blacklisted()) {
    if (tentative_decision == DecisionResponse::BASE ||
        tentative_decision == DecisionResponse::DEDUP) {
      return DecisionResponse::WARM;
    }
  }

  return tentative_decision;
}

DecisionResponse::Decision ControllerServerImpl::DedupHeuristicOpenwhisk(
    int cid, string appl, string env) {
  // Increase the threshold number of decisions before purging container
  // Heuristic policy with base Openwhisk policy
  auto stats = data_structures_->GetEnvironmentStats(env);
  auto appl_info = data_structures_->GetApplicationInfo(appl);
  Decision tentative_decision;

  auto c_info = data_structures_->GetContainerInfo(cid);
  auto m_info = data_structures_->GetMachineInfo(c_info->machine_id());

  // Use the start-up times directly (In seconds)
  double reuse = appl_info->exec_time_ + data_structures_->reuse_period();
  double warm_reuse_period = (stats->warm_start_times_ + reuse) / 1000;
  double dedup_reuse_period = (stats->dedup_start_times_ + reuse) / 1000;
  double provisioned_req = 0.0;
  if (provisioned_ == 0) {
    // Use only warm containers
    provisioned_req =
        ((double)(stats->num_warm_ + stats->num_base_) / warm_reuse_period);
  } else {
    // Use warm+dedup containers
    provisioned_req =
        ((double)(stats->num_warm_ + stats->num_base_) / warm_reuse_period) +
        ((double)stats->num_dedup_ / dedup_reuse_period);
  }

  // For deduplication -- use the same policy
  double lower_threshold = data_structures_->GetMovingWindowArrivalRate(env);

  // Purge only if exceeded the number of decision limits
  // int threshold = 10;
  // if (c_info->num_decisions() > gamma_ * threshold) {
  //   tentative_decision = DecisionResponse::PURGE;
  // } else {
  //   c_info->IncrementNumDecisions();
  //   if (provisioned_req > lower_threshold && base_ready_) {
  //     tentative_decision = DecisionResponse::DEDUP;
  //   } else {
  //     tentative_decision = DecisionResponse::WARM;
  //   }
  // }

  // Check if it is already a base container
  if (c_info->is_base()) {
    return DecisionResponse::BASE;
  }

  // Check if more BASE containers needed, and container is not dedup
  if (stats->num_dedup_ >= stats->num_base_ * dedup_per_base_) {
    if (!c_info->is_dedup()) {
      stats->num_base_++;
      return DecisionResponse::BASE;
    }
  }

  // Check if tentative decision is feasible := DEDUP to WARM not possible
  if (c_info->is_dedup() && tentative_decision == DecisionResponse::WARM) {
    return DecisionResponse::DEDUP;
  }

  return tentative_decision;
}

Status ControllerServerImpl::GetDecision(ServerContext* ctxt,
                                         const Container* args,
                                         DecisionResponse* resp) {
  // BOOST_LOG_TRIVIAL(trace) << "In GetDecision() function";
  int container_id = args->containerid();
  shared_ptr<ContainerInfo> c_info;

  try {
    c_info = data_structures_->GetContainerInfo(container_id);
  } catch (...) {
    BOOST_LOG_TRIVIAL(error) << "GetDecision " << container_id << " cancelled.";
    return Status(StatusCode::CANCELLED, "Container already purged.");
  }

  string appl = c_info->appl();
  string env = c_info->env();
  auto appl_info = data_structures_->GetApplicationInfo(appl);

  // If the container is already dummy -- do not send any decision
  auto container_status = c_info->status();
  if (container_status == kDummy) {
    return Status(StatusCode::CANCELLED, "Container already in a DUMMY state.");
  }

  // TODO: Improvise heuristic
  DecisionResponse::Decision decision;
  // if (container_id % 2 == 0) {
  //   decision = DecisionResponse::BASE;
  // } else {
  //   decision = DecisionResponse::DEDUP;
  // }
  if (appl_info->policy_ == kOpenwhisk) {
    decision = DedupNoneOpenwhisk(container_id, env);
  } else if (appl_info->policy_ == kHeuristicOpenwhisk) {
    decision = DedupHeuristicOpenwhisk(container_id, appl, env);
  } else if (appl_info->policy_ == kNone) {
    decision = DedupNoneHeuristic(container_id, appl, env);
  } else if (appl_info->policy_ == kHeuristic) {
    decision = DedupHeuristic(container_id, appl, env);
  } else if (appl_info->policy_ == kBoundary) {
    decision = DedupBoundaryHeuristic(container_id, appl, env);
  }

  switch (decision) {
    case DecisionResponse::BASE:
    case DecisionResponse::DEDUP:
    case DecisionResponse::WARM:
      // Will be updated when the machine responds
      data_structures_->UpdateContainerStatus(c_info, kDummy);
      break;

    case DecisionResponse::PURGE:
      data_structures_->RemoveContainer(container_id);
      break;

    default:
      break;
  }
  resp->set_decision(decision);
  BOOST_LOG_TRIVIAL(info) << "Decision for " << container_id << " " << decision;

  return Status::OK;
}

// RPC called to notify of a blacklisted container
Status ControllerServerImpl::Blacklist(ServerContext* ctxt,
                                       const Container* args, Ack* ack) {
  int container_id = args->containerid();
  auto c_info = data_structures_->GetContainerInfo(container_id);
  c_info->set_blacklisted();
  return Status::OK;
}

// RPC called to notify the successful change of Status of a container
Status ControllerServerImpl::UpdateStatus(ServerContext* ctxt,
                                          const StatusArgs* args, Ack* ack) {
  int container_id = args->containerid();
  BOOST_LOG_TRIVIAL(info) << "Update status for " << container_id;

  try {
    auto c_info = data_structures_->GetContainerInfo(container_id);
    StatusArgs::Status status = args->status();

    if (status == StatusArgs_Status::StatusArgs_Status_BASE) {
      data_structures_->UpdateContainerStatus(c_info, kBase);
    } else if (status == StatusArgs_Status::StatusArgs_Status_DEDUP) {
      data_structures_->UpdateContainerStatus(c_info, kDedup);
    } else {
      // Used to calculate the number of partial dedup containers
      BOOST_LOG_TRIVIAL(info) << "Update warm container " << container_id;
      data_structures_->UpdateContainerStatus(c_info, kWarm);
    }
  } catch (...) {
    // Implies that the container was already purged.
    BOOST_LOG_TRIVIAL(error)
        << "Container " << container_id << " does not exist";
    return Status::CANCELLED;
  }

  return Status::OK;
}

Status ControllerServerImpl::UpdateAvailableMemory(ServerContext* ctxt,
                                                   const MemoryArgs* args,
                                                   Ack* ack) {
  // Update memory in machine_map_
  int machine_id = args->machine();
  float used_mem = args->usedmemory();

  data_structures_->GetMachineInfo(machine_id)->set_used_memory(used_mem);

  return Status::OK;
}

// RPC called by a BASE container
Status ControllerServerImpl::RegisterPages(ServerContext* ctxt,
                                           const ContainerPages* args,
                                           Ack* ack) {
  // BOOST_LOG_TRIVIAL(trace) << "In RegisterPages() function";

  // Register the hashes from ContainerPages in the global hash table
  int container_id = args->containerid();
  int machine_id = args->machineid();
  int num_pages = args->payload_size();

  int num_hashes;
  for (int i = 0; i < num_pages; i++) {
    PageStruct page_struct = args->payload(i);
    unsigned long int page_addr = page_struct.addr();
    unsigned long int mr_id = page_struct.mrid();
    num_hashes = page_struct.hashes_size();
    auto chunk =
        make_shared<ChunkHashInfo>(container_id, machine_id, page_addr, mr_id);
    for (int j = 0; j < num_hashes; j++) {
      data_structures_->AddChunkHash(page_struct.hashes(j), chunk);
    }
  }

  if (!base_ready_) {
    base_ready_ = true;
  }

  return Status::OK;
}

// RPC called by a DEDUP container
Status ControllerServerImpl::GetBaseContainers(ServerContext* ctxt,
                                               const ContainerPages* args,
                                               BaseContainerResponse* resp) {
  // BOOST_LOG_TRIVIAL(trace) << "In GetBaseContainers() function";

  // Register the hashes from ContainerPages in the global hash table
  int container_id = args->containerid();
  int machine_id = args->machineid();
  int num_pages = args->payload_size();

  // Collect stats about dedup
  int same_func_pages = 0;
  int diff_func_pages = 0;
  auto c_info = data_structures_->GetContainerInfo(container_id);
  string c_appl = c_info->appl();

  // Evaluate the timings
  auto curr_time = system_clock::now();

  /* 1. Iterate over all the pages in the argument and get the hashes of chunks
   * on the page.
   * 2. For each page, compute the heuristic for each distinct base container.
   * 3. Greedily choose base container for each page and add into the
   * BaseContainerResponse object.
   */

  // Map of base containers
  unordered_map<int, int> base_containers;
  int num_dedup_pages = 0;

  for (int i = 0; i < num_pages; i++) {
    PageStruct page_struct = args->payload(i);
    unsigned long int page_addr = page_struct.addr();
    int num_hashes = page_struct.hashes_size();

    // Declare a map for storing duplication information, as follows:
    // Base Container ID versus information of base container pages.
    // NOTE: For each base container - the first match page is taken
    unordered_map<int, shared_ptr<ChunkHashInfo>> dupl_map;

    // List of prospective base containers and their heuristics
    unordered_map<int, double> heursitic_values;

    bool hit = false;
    for (int j = 0; j < num_hashes; j++) {
      // Add the vector of containers to the map for duplication
      vector<shared_ptr<ChunkHashInfo>> addr_vector =
          data_structures_->FindHash(page_struct.hashes(j));
      for (auto& chunk : addr_vector) {
        int base_cid = chunk->cid();
        heursitic_values[base_cid] = 0;

        // If a dedup page has already been found for this page in the base
        // container, do not push another page
        auto search_page = dupl_map.find(base_cid);
        if (search_page != dupl_map.end()) {
          continue;
        }

        hit = true;
        dupl_map.insert(make_pair(base_cid, chunk));
      }
    }

    // Continue if no match for any hash found
    if (!hit) {
      continue;
    }

    // Calculate heuristics for each of the base containers
    double max_heuristic = -1;
    int best_container_id = -1;
    for (auto& base_pair : heursitic_values) {
      int base_cid = base_pair.first;
      auto base_info = data_structures_->GetContainerInfo(base_cid);

      // COMPLETED: Calculate heuristic
      double heuristic = 0.0;
      // heuristic += base_choice_weights_.at(0) * dupl_map[base_cid].size();
      heuristic += base_choice_weights_.at(1) * base_info->refcount();
      if (machine_id == base_info->machine_id()) {
        heuristic += base_choice_weights_.at(2);
      }

      if (heuristic > max_heuristic) {
        max_heuristic = heuristic;
        best_container_id = base_cid;
      }
    }

    auto base_info = dupl_map[best_container_id];
    BasePage* base_page = resp->add_basepagelist();
    base_page->set_addr(page_addr);
    base_page->set_machineid(base_info->mid());
    base_page->set_mrid(base_info->mr_id());
    base_page->set_baseaddr(base_info->addr());

    auto base_cinfo = data_structures_->GetContainerInfo(best_container_id);
    string base_appl = base_cinfo->appl();
    if (base_appl == c_appl) {
      same_func_pages++;
    } else {
      diff_func_pages++;
    }

    // Add to base_containers map to update refcount later
    base_containers[best_container_id] = 0;
    num_dedup_pages++;
  }

  for (auto& base_pair : base_containers) {
    auto base_info = data_structures_->GetContainerInfo(base_pair.first);
    base_info->IncrementRefcount();
  }

  BOOST_LOG_TRIVIAL(trace) << "Num dedup pages: " << num_dedup_pages;

  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - curr_time)
                       .count();

  if (num_dedup_pages != 0) {
    BOOST_LOG_TRIVIAL(trace) << "Same func pages: "
                            << ((float)same_func_pages / num_dedup_pages);
    BOOST_LOG_TRIVIAL(trace) << "Diff func pages: "
                            << ((float)diff_func_pages / num_dedup_pages);
  }

  return Status::OK;
}

void ControllerServerImpl::SpawnSingleContainer(int mid) {
  // TODO: Which container to spawn? Application & execution environment?
  int cid = data_structures_->AddNewContainer(mid, "1", "3");
  MemoryResponse mr;
  SpawnArgs args;
  ClientContext ctx;

  args.set_containerid(cid);
  args.set_application("1");
  args.set_environment("3");
  Status rpc_status =
      data_structures_->GetMachineInfo(mid)->stub_->Spawn(&ctx, args, &mr);

  BOOST_LOG_TRIVIAL(info) << "Added a new container on machine " << mid;

  // Update memory for machine
  data_structures_->GetMachineInfo(mid)->set_used_memory(mr.usedmemory());

  if (!rpc_status.ok()) {
    data_structures_->RemoveContainer(cid);
  }
}

void ControllerServerImpl::SpawnContainers() {
  // Iterate continuously
  while (true) {
    // If more than 20% memory is available on machine then, we should spawn a
    // new container.
    for (int m = 0; m < num_machines_; m++) {
      auto m_info = data_structures_->GetMachineInfo(m);
      if (m_info->has_enough_memory()) {
        // Spawn a new container on the thread pool
        post(workers_,
             std::bind(&ControllerServerImpl::SpawnSingleContainer, this, m));
      } else {
        // BOOST_LOG_TRIVIAL(info)
        //     << "Machine " << m << " is out of memory " << m_info->memory()
        //     << " " << m_info->total_memory_;
      }
    }

    // Sleep for a little while before checking for more space again.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

/**
 * Read the requests from a file, and pass to the Controller as per timestamp.
 * Request file format: <timestamp (in ms)> <application> <execution env>
 */
void ControllerServerImpl::ServeRequests(string filename) {
  BOOST_LOG_TRIVIAL(debug) << "Starting workload trace";
  ifstream request_file(filename);

  int reqs = 0;
  string timestamp, appl, exec_env;
  string next_timestamp, next_appl, next_env;
  request_file >> timestamp >> appl >> exec_env;
  while (request_file >> next_timestamp >> next_appl >> next_env) {
    // Update the arrival rates stats
    auto curr_time = system_clock::now();
    data_structures_->UpdateArrivalRates(exec_env, curr_time);

    // Spawn thread for the current <timestamp, env>
    post(workers_, std::bind(&ControllerServerImpl::Scheduler::ScheduleRequest,
                             scheduler_.get(), appl, exec_env));
    reqs++;

    if (stoi(next_timestamp) == -1) {
      // Wait for all requests to complete execution
      BOOST_LOG_TRIVIAL(trace) << "Closing up execution now";
      while ((reqs - scheduler_->completed_reqs_) > 20) {
        BOOST_LOG_TRIVIAL(trace)
            << "Completed reqs: " << scheduler_->completed_reqs_ << " " << reqs;
        std::this_thread::sleep_for(std::chrono::duration<double>(10));
      }

      // No further requests. Terminate all. But sleep for ten seconds to ensure
      // any pending operations on machines.
      std::this_thread::sleep_for(std::chrono::duration<double>(10));

      for (int mid = 0; mid < num_machines_; mid++) {
        Ack ack;
        Ack args;
        ClientContext ctx;
        data_structures_->GetMachineInfo(mid)->stub_->Terminate(&ctx, args,
                                                                &ack);
      }

      // Before terminating: print the number of dropped reqs
      BOOST_LOG_TRIVIAL(info)
          << "Dropped requests: " << scheduler_->dropped_reqs_;

      // Wait for another couple seconds and exit
      std::this_thread::sleep_for(std::chrono::duration<double>(2));
      exit(0);
    }

    // Read next line and sleep for the time difference
    int sleep_time = stoi(next_timestamp) - stoi(timestamp);
    // BOOST_LOG_TRIVIAL(debug) << "Sleeping for " << sleep_time;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));

    // Switch current timestamp and exec_env
    timestamp = next_timestamp;
    appl = next_appl;
    exec_env = next_env;
  }
}

void ControllerServerImpl::SetupConnections() {
  boost::property_tree::ptree tree;
  boost::property_tree::read_json("config/cluster.json", tree);

  BOOST_FOREACH (boost::property_tree::ptree::value_type& v,
                 tree.get_child("grpc_nodes")) {
    int mid = v.second.get<int>("machine_id");
    string addr = v.second.get<string>("addr");
    string port = v.second.get<string>("port");
    string conn = addr + ":" + port;

    BOOST_LOG_TRIVIAL(debug)
        << "Opening connection with: " << conn << " " << mid;

    data_structures_->AddMachineInfo(mid, conn, mem_cap_);
    num_machines_ += 1;
  }
}

void ControllerServerImpl::ReadConfig() {
  INIReader reader("config/controller.ini");

  // Read heuristic weights from config file
  string base_str = reader.Get("heuristics", "basechoice", "");
  string state_str = reader.Get("heuristics", "statepolicy", "");

  string token;
  istringstream base_ss(base_str);
  while (base_ss >> token) {
    base_choice_weights_.push_back(std::stod(token));
  }

  istringstream state_ss(state_str);
  while (state_ss >> token) {
    state_policy_weights_.push_back(std::stod(token));
  }

  // Read default policy
  Policy default_policy =
      static_cast<Policy>(reader.GetInteger("policy", "policy", 2));
  data_structures_->set_default_policy(default_policy);
  latency_constraint_ =
      reader.GetInteger("policy", "constraint", 1) == 1 ? true : false;

  dedup_per_base_ = reader.GetInteger("policy", "dedupperbase", 10);
  threshold_ = reader.GetFloat("policy", "threshold", 10.0);
  alpha_ = reader.GetFloat("policy", "alpha", 4);
  beta_ = reader.GetFloat("policy", "beta", 10);
  gamma_ = reader.GetFloat("policy", "gamma", 2);
  provisioned_ = reader.GetInteger("policy", "provisioned", 0);

  // Read params
  data_structures_->set_reuse_period(
      reader.GetInteger("params", "reuseperiod", 500));
  data_structures_->set_request_window(
      reader.GetInteger("params", "window", 10));
  mem_cap_ = (float)reader.GetInteger("params", "memcap", 500);

  // Read application configuration from agent config
  INIReader agent_reader("config/agent.ini");

  data_structures_->set_idle_period(
      agent_reader.GetInteger("parameters", "idletime", 60));

  int num_env = agent_reader.GetInteger("configuration", "numenv", 4);
  int num_appl = agent_reader.GetInteger("configuration", "numappl", 1);
  for (int i = 0; i < num_appl; i++) {
    string appl_name = "appl" + std::to_string(i);
    string conf_str = agent_reader.Get("configuration", appl_name, "");

    string keep_alive, exec_time, memory, dedup_benefit;
    istringstream conf_ss(conf_str);
    conf_ss >> keep_alive;
    conf_ss >> exec_time;
    conf_ss >> memory;
    conf_ss >> dedup_benefit;

    data_structures_->AddNewApplication(
        std::to_string(i), std::stoi(keep_alive), std::stoi(exec_time));
    data_structures_->AddNewEnvironment(std::to_string(i), std::stod(memory),
                                        std::stod(dedup_benefit));
  }
}