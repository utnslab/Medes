#include "data_structures.h"

#include <boost/log/trivial.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "defs.h"

using std::make_pair;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::system_clock;
using std::chrono::time_point;

void DataStructures::UpdateContainerStatus(shared_ptr<ContainerInfo> c_info,
                                           ContainerStatus status) {
  // Update the policy stat too
  ContainerStatus prev_status = c_info->status();
  c_info->UpdateStatus(status);

  UpdateEnvStats(c_info, prev_status, false);
}

void DataStructures::AddChunkHash(string hash,
                                  shared_ptr<ChunkHashInfo> chunk) {
  // Guard shared access to hash table.
  boost::unique_lock<boost::shared_mutex> lock(hash_table_mutex_);

  // [] operator creates a new entry if does not exist.
  hash_table_[hash].push_back(chunk);
}

vector<shared_ptr<ChunkHashInfo>> DataStructures::FindHash(string hash) {
  boost::shared_lock<boost::shared_mutex> lock(hash_table_mutex_);
  auto search_hash = hash_table_.find(hash);
  if (search_hash != hash_table_.end()) {
    return search_hash->second;
  }

  // If not found, return an empty vector
  vector<shared_ptr<ChunkHashInfo>> temp_vec;
  return temp_vec;
}

int DataStructures::AddNewContainer(int machine_id, string appl, string env) {
  boost::unique_lock<boost::shared_mutex> lock(container_map_mutex_);

  int cid = global_cid_++;
  auto c_info = make_shared<ContainerInfo>(cid, machine_id, appl, env);
  container_map_.insert(make_pair(cid, c_info));

  // Update policy stats
  UpdateEnvStats(c_info, kDummy, false);
  return cid;
}

void DataStructures::RemoveContainer(int container_id) {
  auto c_info = GetContainerInfo(container_id);
  UpdateEnvStats(c_info, c_info->status(), true);

  boost::unique_lock<boost::shared_mutex> lock(container_map_mutex_);
  container_map_.erase(container_id);
}

double DataStructures::GetEnvMemoryFraction(std::string env) {
  double total_memory =
      0.95 * machine_map_.at(0)->total_memory_ * machine_map_.size();
  double total_requests = 0.0;
  for (auto &policy_pair : policy_stats_) {
    total_requests += GetMaxArrivalRate(policy_pair.first);
  }
  double fraction = GetMaxArrivalRate(env) / total_requests;
  return fraction * total_memory;
}

int DataStructures::GetMaxMemoryMachine() {
  int max_id = 0;
  int max_memory = 0;
  for (int i = 0; i < machine_map_.size(); i++) {
    if (max_memory < machine_map_.at(i)->memory()) {
      max_memory = machine_map_.at(i)->memory();
      max_id = i;
    }
  }

  return max_id;
}

int DataStructures::RoundRobinMachine() {
  std::lock_guard<std::mutex> guard(round_robin_mutex_);
  int mid = round_robin_;
  round_robin_ = (round_robin_ + 1) % machine_map_.size();

  int num_tries = 0;
  while (!machine_map_.at(mid)->has_enough_memory()) {
    mid = round_robin_;
    round_robin_ = (round_robin_ + 1) % machine_map_.size();

    num_tries++;
    if (num_tries > machine_map_.size()) return -1;
  }

  return mid;
}

int DataStructures::GetAvailableContainer(string appl, string exec_env,
                                          double time_since_arrival) {
  // NOTE: Does not use next assigned and SLO aware scheduling for now.
  int warm_id = -1;
  int dedup_id = -1;
  int running_id = -1;

  {
    boost::shared_lock<boost::shared_mutex> lock(container_map_mutex_);
    auto curr_time = system_clock::now();
    for (auto &container : container_map_) {
      int cid = container.first;
      auto c_info = container.second;

      // Only consider containers of a particular application type
      if (c_info->env() != exec_env) continue;

      // Consider container if it has been atleast reuse_period_ since last mod.
      auto last_mod = duration_cast<duration<double, std::milli>>(
                          curr_time - c_info->last_modified())
                          .count();

      ContainerStatus status = c_info->status();
      bool next_assigned = c_info->next_assigned();

      if (status != kRunning) {
        // Allow for a small buffer period
        if (last_mod < reuse_period_) continue;

        // Check if container is around decision time.
        if (last_mod > (idle_period_ - 2) * 1000 &&
            last_mod < (idle_period_ + 5) * 1000)
          continue;

        // If next_assigned is set then another request is waiting to be
        // executed on this container.
        if (warm_id == -1 && (status == kWarm || status == kBase) &&
            !next_assigned) {
          warm_id = cid;
        } else if (dedup_id == -1 && status == kDedup) {
          dedup_id = cid;
        }
      } else {
        if (running_id == -1 && status == kRunning && !next_assigned) {
          // Check if this container can actually meet SLO - Tentative
          // scheduling delay should be less than the max permissible scheduling
          // delay int exec_time = GetApplicationInfo(appl)->exec_time_; if
          // (time_since_arrival + (exec_time + reuse_period_ - last_mod) >
          //     (GetApplicationInfo(appl)->slo_ - exec_time))
          //   continue;
          running_id = cid;
        }
      }

      if (dedup_id != -1 && warm_id != -1) {
        break;
      }
    }
  }

  // NOTE: Prefer warm container over dedup container for now.
  if (warm_id != -1) {
    return warm_id;
  } else if (dedup_id != -1) {
    return dedup_id;
  }
  return -1;
}

string DataStructures::GetEvictionEnvironment() {
  // NOTE: Find the environment with the maximum (warm containers/request rate)
  // ratio
  double best_ratio = 0.0;
  string eviction_env = "";
  for (auto &appl_pair : policy_stats_) {
    double rate = GetMovingWindowArrivalRate(appl_pair.first);
    double ratio = appl_pair.second->num_warm_ / rate;
    if (ratio > best_ratio) {
      best_ratio = ratio;
      eviction_env = appl_pair.first;
    }
  }

  return eviction_env;
}

int DataStructures::GetEvictionCandidate(string exec_env) {
  int eviction_candidate = -1;
  double max_time = 0.0;
  ContainerStatus status;

  {
    boost::shared_lock<boost::shared_mutex> lock(container_map_mutex_);
    auto curr_time = system_clock::now();
    for (auto &container : container_map_) {
      int cid = container.first;
      auto c_info = container.second;

      // Only consider containers of a particular application type
      if (c_info->env() != exec_env) continue;

      // Consider container if it has been atleast reuse_period_ since last mod.
      auto last_mod = duration_cast<duration<double, std::milli>>(
                          curr_time - c_info->last_modified())
                          .count();

      // Check if container is around decision time.
      if (last_mod > (idle_period_ - 2) * 1000 &&
          last_mod < (idle_period_ + 2) * 1000)
        continue;

      // Check if container just got into this state
      if (last_mod < 2000)
        continue;

      status = c_info->status();

      if (status == kWarm || status == kDedup) {
        if (!c_info->next_assigned() && last_mod > max_time) {
          eviction_candidate = cid;
          max_time = last_mod;
        }
      }
    }
  }

  return eviction_candidate;
}

void DataStructures::UpdateStartupTimes(string env, double start_time,
                                        bool is_warm) {
  auto stats = policy_stats_.at(env);
  if (is_warm) {
    stats->warm_start_times_ = stats->alpha_ * start_time +
                               (1 - stats->alpha_) * stats->warm_start_times_;
  } else {
    stats->dedup_start_times_ = stats->alpha_ * start_time +
                                (1 - stats->alpha_) * stats->dedup_start_times_;
  }
}

double DataStructures::GetMaxArrivalRate(string env) {
  auto stats = policy_stats_.at(env);

  int max_requests = 0;
  double time_diff = 0;
  auto curr_time = system_clock::now();
  {
    boost::shared_lock<boost::shared_mutex> lock(stats->rates_mutex_);
    int length = stats->rates_.size();
    for (int i = length - 1; i >= 0; i--) {
      auto td = duration_cast<duration<double, std::milli>>(
                    curr_time - stats->rates_.at(i).second)
                    .count();
      if (td < request_window_ * 60 * 1000) {
        if (max_requests < stats->rates_.at(i).first) {
          max_requests = stats->rates_.at(i).first;
        }
      } else {
        break;
      }
    }
  }

  // Return the maximum arrival rate in the past window (in requests per second)
  return (double)max_requests / 60;
}

double DataStructures::GetMovingWindowArrivalRate(string env) {
  auto stats = policy_stats_.at(env);

  int requests = 0;
  double time_diff = 0.0;
  auto curr_time = system_clock::now();
  {
    boost::shared_lock<boost::shared_mutex> lock(stats->rates_mutex_);
    int length = stats->rates_.size();
    for (int i = length - 1; i >= 0; i--) {
      auto td = duration_cast<duration<double, std::milli>>(
                    curr_time - stats->rates_.at(i).second)
                    .count();
      if (td < request_window_ * 60 * 1000) {
        time_diff = td;
        requests += stats->rates_.at(i).first;
      } else {
        break;
      }
    }
  }

  // Return the moving avg arrival rate in the past window (requests per sec)
  // BOOST_LOG_TRIVIAL(trace) << "Time diff: " << time_diff << " req: " <<
  // requests;
  if (requests == 0) {
    return 0;
  }
  return ((double)requests * 1000) / time_diff;
}

void DataStructures::UpdateArrivalRates(string env,
                                        time_point<system_clock> curr_time) {
  auto stats = policy_stats_.at(env);

  // Check if it has been a minute since the last time, rate window was updated.
  if (stats->rates_.size() > 0) {
    auto time_diff = duration_cast<duration<double, std::milli>>(
                         curr_time - stats->rates_.back().second)
                         .count();
    // Update request rates in the past minute
    if (time_diff > 60 * 1000) {
      // Avoid multiple threads adding and removing elements together
      boost::unique_lock<boost::shared_mutex> lock(stats->rates_mutex_);
      stats->rates_.push_back(
          std::make_pair(stats->requests_in_last_minute_, curr_time));
      stats->requests_in_last_minute_ = 0;

      // No need to check the timings here - the arrival rates calculation shall
      // take care of that.
      if (stats->rates_.size() > request_window_) {
        stats->rates_.erase(stats->rates_.begin());
      }
    }
  } else {
    stats->rates_.push_back(std::make_pair(0, curr_time));
  }

  stats->requests_in_last_minute_++;
}

void DataStructures::UpdateEnvStats(shared_ptr<ContainerInfo> c_info,
                                    ContainerStatus prev_status,
                                    bool purge = false) {
  ContainerStatus new_status = kDummy;
  if (!purge) {
    new_status = c_info->status();
  }
  string env = c_info->env();

  // Creates appl-policy if not created before
  shared_ptr<EnvironmentInfo> stats = GetEnvironmentStats(env);

  // Update the warm/dedup numbers based on previous status and next status
  // NOTE: Ignore running status containers for policy decisions
  if (new_status != prev_status) {
    if (prev_status == kWarm || prev_status == kBase ||
        prev_status == kRunning) {
      stats->num_warm_--;
    } else if (prev_status == kDedup) {
      stats->num_dedup_--;
    }

    if (new_status == kWarm || new_status == kBase || new_status == kRunning) {
      stats->num_warm_++;
    } else if (new_status == kDedup) {
      stats->num_dedup_++;
    }
  }

  // Number of base containers change only if a container is being purged or a
  // new base container is being added
  bool is_base = c_info->is_base();
  if (purge && is_base) {
    stats->num_base_--;
  }
}
