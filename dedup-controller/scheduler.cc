// Scheduling functions here

#include <boost/log/trivial.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "controller_server.h"

using dedup::Ack;
using dedup::MemoryResponse;
using dedup::SpawnArgs;
using grpc::ClientContext;
using std::shared_ptr;
using std::string;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::system_clock;
using std::chrono::time_point;

void ControllerServerImpl::Scheduler::ScheduleRequest(string appl,
                                                      string exec_env) {
  // BOOST_LOG_TRIVIAL(trace) << "Starting the schedule request function";

  // Check if any warm or dedup container is available for serving the request.
  int cid;
  bool scheduled = false;
  bool should_evict = false;
  ContainerStatus status;

  // Statistics
  auto start_time = system_clock::now();
  double scheduling_delay = 0.0;
  double latency = 0.0;
  double subtract_from_latency = 0.0;
  bool scheduled_on_dedup = false;
  bool scheduled_on_warm = false;

  int max_tries = 1000;
  int num_tries = 0;

  string scheduled_str = "";

  while (!scheduled) {
    // Find a suitable container
    bool found = false;
    while (!found) {
      BOOST_LOG_TRIVIAL(trace)
          << "Finding available container " << completed_reqs_;
      num_tries += 1;
      if (num_tries > max_tries) {
        break;
      }
      double time_since_arrival = duration_cast<duration<double, std::milli>>(
                                      system_clock::now() - start_time)
                                      .count();
      cid = ds_->GetAvailableContainer(appl, exec_env, time_since_arrival);
      if (cid == -1) {
        status = kDummy;
        found = true;
        break;
      } else {
        auto c_info = ds_->GetContainerInfo(cid);

        // Critical section for locking container to request
        std::lock_guard<std::mutex> guard(scheduler_mutex_);
        status = c_info->status();
        if (status == kDedup || status == kWarm || status == kBase) {
          found = true;
          ds_->UpdateContainerStatus(c_info, ContainerStatus::kDummy);
          break;
        } else if (status == kRunning) {
          found = true;
          bool next_assigned = c_info->next_assigned();
          if (!next_assigned) {
            c_info->set_next_assigned(true);
            break;
          }
        }
        // End critical section
      }
    }

    auto available_delay = duration_cast<duration<double, std::milli>>(
                               system_clock::now() - start_time)
                               .count();
    BOOST_LOG_TRIVIAL(trace)
        << "Available container time: " << appl << " " << available_delay;

    if (num_tries > max_tries) {
      break;
    }

    int machine_id = -1;
    if (cid == -1) {
      // Currently running a max available memory greedy approach
      // machine_id = ds_->GetMaxMemoryMachine();
      int num_evictions = 0;
      int max_evictions = 10;
      machine_id = ds_->RoundRobinMachine();
      while (machine_id == -1 && num_evictions < max_evictions) {
        // Need to start another container -- evict some other container
        BOOST_LOG_TRIVIAL(trace)
            << "Could not find machine. Need to evict container";
        EvictContainer(exec_env);
        machine_id = ds_->RoundRobinMachine();
        num_evictions += 1;
      }

      if (machine_id == -1) {
        // Evictions were not successful in relieving memory pressure. Continue
        // in the next iteration.
        BOOST_LOG_TRIVIAL(trace) << "Machine evictions not enough.";
        continue;
      }

      // Add a new container to the container map
      int cid = ds_->AddNewContainer(machine_id, appl, exec_env);

      // Scheduling complete
      scheduling_delay = duration_cast<duration<double, std::milli>>(
                             system_clock::now() - start_time)
                             .count();

      // Send spawn request to the respective machine
      MemoryResponse mr;
      SpawnArgs args;
      ClientContext ctx;

      // Set the deadline for the RPC call
      auto deadline = system_clock::now() + std::chrono::milliseconds(3500);
      ctx.set_deadline(deadline);

      args.set_containerid(cid);
      args.set_environment(exec_env);
      args.set_application(appl);

      BOOST_LOG_TRIVIAL(trace)
          << "Starting new container " << cid << " on " << machine_id;

      auto curr_time = system_clock::now();
      Status rpc_status =
          ds_->GetMachineInfo(machine_id)->stub_->Spawn(&ctx, args, &mr);
      auto rpc_delay = duration_cast<duration<double, std::milli>>(
                           system_clock::now() - curr_time)
                           .count();
      scheduled_str += "C " + std::to_string(scheduling_delay) + " " +
                       std::to_string(rpc_delay);

      // Update memory for machine
      ds_->GetMachineInfo(machine_id)->set_used_memory(mr.usedmemory());

      // Check result of grpc call and handle failures.
      if (rpc_status.ok()) {
        BOOST_LOG_TRIVIAL(trace)
            << "Cold RPC delay: " << appl << " " << rpc_delay;
        BOOST_LOG_TRIVIAL(info)
            << "Assigned new container " << appl << " " << cid;
        auto c_info = ds_->GetContainerInfo(cid);
        ds_->UpdateContainerStatus(c_info, ContainerStatus::kRunning);
        scheduled = true;
      } else {
        BOOST_LOG_TRIVIAL(error)
            << "Failed new container " << cid << " " << rpc_status.error_code()
            << " " << machine_id;
        BOOST_LOG_TRIVIAL(error) << rpc_status.error_message();
        // Remove that container
        ds_->RemoveContainer(cid);

        // If spawn has failed then there's likely heavy load on machine. So,
        // sleep for some time before trying again.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }

      // Check if we need to evict some container
      if (!ds_->GetMachineInfo(machine_id)->has_enough_memory()) {
        should_evict = true;
      }
    } else {
      auto c_info = ds_->GetContainerInfo(cid);
      machine_id = c_info->machine_id();
      double rpc_delay = 0.0;

      // Send Restore/Restart request depending on container status
      Container container;
      ClientContext ctx;
      Status rpc_status;

      container.set_containerid(cid);
      if (status == ContainerStatus::kDedup) {
        // Set the deadline for the RPC call
        auto deadline = system_clock::now() + std::chrono::milliseconds(5000);
        ctx.set_deadline(deadline);

        // Check that the per machine dedup containers is not violated.
        int num_dedups = ds_->GetMachineInfo(machine_id)->num_dedup_starts();
        auto curr_time = system_clock::now();
        while (num_dedups >= dedup_starts_per_machine_) {
          BOOST_LOG_TRIVIAL(trace)
              << "Waiting for per machine dedup starts " << num_dedups
              << " for machine " << machine_id;
          num_dedups = ds_->GetMachineInfo(machine_id)->num_dedup_starts();
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        auto dedup_per_machine_delay =
            duration_cast<duration<double, std::milli>>(system_clock::now() -
                                                        curr_time)
                .count();
        BOOST_LOG_TRIVIAL(trace)
            << "Dedup delay: " << appl << " " << dedup_per_machine_delay;

        ds_->GetMachineInfo(machine_id)->IncrementDedupStarts();

        // Scheduling complete
        scheduling_delay = duration_cast<duration<double, std::milli>>(
                               system_clock::now() - start_time)
                               .count();

        // Send restore request
        MemoryResponse mr;

        curr_time = system_clock::now();
        rpc_status = ds_->GetMachineInfo(machine_id)
                         ->stub_->Restore(&ctx, container, &mr);
        rpc_delay = duration_cast<duration<double, std::milli>>(
                        system_clock::now() - curr_time)
                        .count();

        scheduled_str += "D " + std::to_string(scheduling_delay) + " " +
                         std::to_string(rpc_delay);

        // Update memory for machine
        ds_->GetMachineInfo(machine_id)->set_used_memory(mr.usedmemory());
        ds_->GetMachineInfo(machine_id)->DecrementDedupStarts();

        // Check if we need to evict some container
        if (!ds_->GetMachineInfo(machine_id)->has_enough_memory()) {
          should_evict = true;
        }

        BOOST_LOG_TRIVIAL(trace) << "Completed dedup start call";
      } else {
        // Set the deadline for the RPC call
        auto deadline = system_clock::now() + std::chrono::milliseconds(2000);
        ctx.set_deadline(deadline);

        // Wait until status becomes kWarm
        if (status == ContainerStatus::kRunning) {
          // NOTE: From RUNNING - container always goes into WARM or BASE state.
          while (status != kWarm && status != kBase) {
            BOOST_LOG_TRIVIAL(trace)
                << "Waiting for warm container to be ready";

            status = c_info->status();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
          }
          // Wait for the reuse buffer period on the assigned container.
          std::this_thread::sleep_for(
              std::chrono::milliseconds(ds_->reuse_period()));
          subtract_from_latency = ds_->reuse_period();
        }

        // Scheduling complete
        scheduling_delay = duration_cast<duration<double, std::milli>>(
                               system_clock::now() - start_time)
                               .count();

        // Send restart request
        MemoryResponse mr;
        BOOST_LOG_TRIVIAL(trace)
            << "Starting warm container " << cid << " " << machine_id;

        auto curr_time = system_clock::now();
        rpc_status = ds_->GetMachineInfo(machine_id)
                         ->stub_->Restart(&ctx, container, &mr);
        rpc_delay = duration_cast<duration<double, std::milli>>(
                        system_clock::now() - curr_time)
                        .count();

        scheduled_str += "W " + std::to_string(scheduling_delay) + " " +
                         std::to_string(rpc_delay);

        ds_->GetMachineInfo(machine_id)->set_used_memory(mr.usedmemory());
      }

      if (rpc_status.ok()) {
        // Log startup
        if (status == ContainerStatus::kDedup) {
          scheduled_on_dedup = true;
          BOOST_LOG_TRIVIAL(trace)
              << "Dedup RPC delay: " << appl << " " << rpc_delay;
        } else {
          scheduled_on_warm = true;
          BOOST_LOG_TRIVIAL(trace)
              << "Warm RPC delay: " << appl << " " << rpc_delay;
        }

        // Update container status
        // NOTE: Startup states are not maintained at the controller as that is
        // handled by the Dedup Agents on each of the machines.
        ds_->UpdateContainerStatus(c_info, ContainerStatus::kRunning);
        // Reset number of decisions
        // c_info->ResetNumDecisions();
        c_info->ResetIdleState();
        c_info->set_next_assigned(false);
        scheduled = true;
      } else {
        if (status == ContainerStatus::kDedup) {
          BOOST_LOG_TRIVIAL(error)
              << "Dedup container failed " << appl << " " << machine_id << " "
              << rpc_status.error_code();
          BOOST_LOG_TRIVIAL(trace) << rpc_status.error_message();
        } else {
          BOOST_LOG_TRIVIAL(error)
              << "Warm container failed " << cid << " " << machine_id << " "
              << rpc_status.error_code();
          BOOST_LOG_TRIVIAL(trace) << rpc_status.error_message();
          c_info->IncrementNumFailed();
          if (c_info->num_failed() >= num_failures_) {
            // Purge container on the machine
            Container container;
            ClientContext ctx;
            container.set_containerid(cid);

            MemoryResponse mr;
            Status rpc_status = ds_->GetMachineInfo(machine_id)
                                    ->stub_->Purge(&ctx, container, &mr);
            ds_->GetMachineInfo(machine_id)->set_used_memory(mr.usedmemory());

            ds_->RemoveContainer(cid);
          }
        }

        ds_->UpdateContainerStatus(c_info, status);
        c_info->set_next_assigned(false);
      }
    }
  }

  latency = duration_cast<duration<double, std::milli>>(system_clock::now() -
                                                        start_time)
                .count();

  // Update startup rates and log statistics
  if (scheduled_on_warm) {
    ds_->UpdateStartupTimes(exec_env, latency - subtract_from_latency, true);
    BOOST_LOG_TRIVIAL(info)
        << "Warm Scheduling delay " << appl << " " << scheduling_delay;
    BOOST_LOG_TRIVIAL(info) << "Warm start time " << appl << " " << latency;
  } else if (scheduled_on_dedup) {
    ds_->UpdateStartupTimes(exec_env, latency - subtract_from_latency, false);
    BOOST_LOG_TRIVIAL(info)
        << "Dedup Scheduling delay " << appl << " " << scheduling_delay;
    BOOST_LOG_TRIVIAL(info) << "Dedup start time " << appl << " " << latency;
  } else {
    BOOST_LOG_TRIVIAL(info)
        << "Cold Scheduling delay " << appl << " " << scheduling_delay;
    BOOST_LOG_TRIVIAL(info) << "Cold start time " << appl << " " << latency;
  }
  BOOST_LOG_TRIVIAL(info) << "Breakdown " << cid << ": " << scheduled_str;

  // Add to the completed requests number
  {
    std::lock_guard<std::mutex> guard(completed_mutex_);
    completed_reqs_ += 1;
  }
  BOOST_LOG_TRIVIAL(trace) << "Completed reqs: " << completed_reqs_;

  // if (should_evict) {
  //   BOOST_LOG_TRIVIAL(info) << "Need to evict container";
  //   EvictContainer(exec_env);
  // }
}

void ControllerServerImpl::Scheduler::EvictContainer(string exec_env) {
  // NOTE: The function currently uses the exec_env to purge a contaoiner of the
  // corresponding execution environment. Change to evicting a container of anyu
  // application.
  int cid = -1;
  ContainerStatus status;
  bool found = false;
  auto curr_time = system_clock::now();

  int num_tries = 0;
  int max_tries = 20;

  while (!found) {
    string eviction_env = ds_->GetEvictionEnvironment();
    cid = ds_->GetEvictionCandidate(eviction_env);
    // BOOST_LOG_TRIVIAL(trace) << "Checking eviction candidate: " << cid;
    if (cid != -1) {
      shared_ptr<ContainerInfo> c_info;
      try {
        c_info = ds_->GetContainerInfo(cid);
      } catch (...) {
        continue;
      }

      // Critical section for locking container to request
      // BOOST_LOG_TRIVIAL(trace) << "Waiting to enter CS 2";
      std::lock_guard<std::mutex> guard(scheduler_mutex_);
      status = c_info->status();
      if ((status == kDedup || status == kWarm) && !c_info->next_assigned()) {
        found = true;
        ds_->UpdateContainerStatus(c_info, ContainerStatus::kDummy);
        break;
      }
      // End critical section
    }

    num_tries += 1;
    if (num_tries > max_tries) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (num_tries > max_tries) {
    return;
  }

  auto eviction_delay = duration_cast<duration<double, std::milli>>(
                            system_clock::now() - curr_time)
                            .count();
  BOOST_LOG_TRIVIAL(trace) << "Eviction candidate " << cid
                           << " delay: " << eviction_delay;

  if (cid != -1) {
    // Purge container
    auto c_info = ds_->GetContainerInfo(cid);
    int machine_id = c_info->machine_id();

    Container container;
    ClientContext ctx;
    container.set_containerid(cid);

    MemoryResponse mr;
    Status rpc_status =
        ds_->GetMachineInfo(machine_id)->stub_->Purge(&ctx, container, &mr);
    ds_->GetMachineInfo(machine_id)->set_used_memory(mr.usedmemory());

    if (rpc_status.ok()) {
      ds_->RemoveContainer(cid);
      if (status == kWarm) {
        BOOST_LOG_TRIVIAL(info) << "Evicted container " << cid << " warm";
      } else {
        BOOST_LOG_TRIVIAL(info) << "Evicted container " << cid << " dedup";
      }

      eviction_delay = duration_cast<duration<double, std::milli>>(
                           system_clock::now() - curr_time)
                           .count();
      BOOST_LOG_TRIVIAL(trace) << "Eviction delay: " << eviction_delay;
    } else {
      BOOST_LOG_TRIVIAL(error)
          << "Eviction failed " << rpc_status.error_code() << " " << machine_id;
      BOOST_LOG_TRIVIAL(error) << rpc_status.error_message();
    }
  }

  // BOOST_LOG_TRIVIAL(trace) << "Exiting eviction function";
}