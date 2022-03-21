// Server for the DedupService gRPC server

#include "dedup_server.h"

#include <grpcpp/grpcpp.h>

#include <boost/log/trivial.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "client.grpc.pb.h"
#include "data_structures.h"
#include "dedup_application.h"

using dedup::Ack;
using dedup::Container;
using dedup::DedupService;
using dedup::MemoryResponse;
using dedup::SpawnArgs;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using std::string;
using std::unique_ptr;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::system_clock;

Status DedupServerImpl::Spawn(ServerContext* ctxt, const SpawnArgs* spawn_args,
                              MemoryResponse* resp) {
  // BOOST_LOG_TRIVIAL(trace) << "In Spawn() function";
  if (ctxt->IsCancelled()) {
    return Status(StatusCode::CANCELLED,
                  "Deadline exceeded or Client cancelled, abandoning.");
  }

  int container_id = spawn_args->containerid();
  string env = spawn_args->environment();
  string appl = spawn_args->application();
  string args = std::to_string(container_id) + " " + env;
  BOOST_LOG_TRIVIAL(trace) << "Spawning " << container_id;

  if (container_id > 999999) {
    BOOST_LOG_TRIVIAL(error)
        << "Incorrect arguments for spawning new container";
    return Status(StatusCode::CANCELLED, "Incorrect arguments, abandoning.");
  }

  // If the spawn fails -- wait for certain time and retry for max_tries
  int num_tries = 0;
  int max_tries = 5;
  bool success = false;

  auto start_time = system_clock::now();
  auto trial_start = system_clock::now();

  while (num_tries < max_tries) {
    // If result is not as expected send error message to the controller.
    // NOTE: The following LaunchScript call is blocking and other operations
    // from the controller will be in queue while this operation gets completed.
    trial_start = system_clock::now();
    LaunchScript("spawn", args);
    auto trial_time = duration_cast<duration<double, std::milli>>(
                          system_clock::now() - trial_start)
                          .count();

    if (trial_time < 100) {
      BOOST_LOG_TRIVIAL(error) << "Probable cold start issue " << container_id;
    } else {
      success = true;
      break;
    }

    num_tries++;
  }

  if (!success || ctxt->IsCancelled()) {
    int ret = DockerSingleOperation(container_id, "remove?force=true", 204);
    if (ret == -1) {
      // Try again
      int ret = DockerSingleOperation(container_id, "remove", 204);
    }
    BOOST_LOG_TRIVIAL(info) << "Failed cold start";
    return Status(StatusCode::CANCELLED,
                  "Deadline exceeded or Client cancelled, abandoning.");
  }

  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - start_time)
                       .count();

  BOOST_LOG_TRIVIAL(info) << "Cold start time " << appl << " " << exec_time;

  // Add the newly spawned container to the local_containers_ map.
  if (auto appl_ptr = appl_.lock()) {
    // Try to gather a shared_ptr to the application. If not found --
    // application has crashed - ABORT.
    appl_ptr->AddContainer(container_id, appl);
    resp->set_usedmemory(appl_ptr->CurrentUsedMemory());
  } else {
    BOOST_LOG_TRIVIAL(error) << "Couldn't lock weak ptr";
    return Status(StatusCode::FAILED_PRECONDITION,
                  "Application not running on machine");
  }

  BOOST_LOG_TRIVIAL(trace) << "Started container " << container_id;
  return Status::OK;
}

Status DedupServerImpl::Restart(ServerContext* ctxt, const Container* args,
                                MemoryResponse* resp) {
  // BOOST_LOG_TRIVIAL(trace) << "In Restart() function";
  if (ctxt->IsCancelled()) {
    return Status(StatusCode::CANCELLED,
                  "Deadline exceeded or Client cancelled, abandoning.");
  }

  // Call the shell script to restart, with the container argument
  int container_id = args->containerid();
  ContainerStatus status = container_map_->GetContainerStatus(container_id);

  if (status != kWarm && status != kBase) {
    BOOST_LOG_TRIVIAL(trace) << "Container is not in warm/base state";
    return Status(StatusCode::CANCELLED,
                  "Container is not in warm/base state.");
  }

  // NOTE: The following LaunchScript call is blocking and other operations from
  // the controller will be in queue while this operation gets completed.
  auto start_time = system_clock::now();

  string expected_result = "cont" + std::to_string(container_id) + "\n";
  BOOST_LOG_TRIVIAL(trace) << "Unpause 1 " << container_id;
  int result = DockerSingleOperation(container_id, "unpause", 204);

  // TODO: If result is not as expected send error message to the controller.
  if (result != 0) {
    BOOST_LOG_TRIVIAL(info)
        << "Returning cancelled. Could not warm start " << container_id;
    return Status::CANCELLED;
  }

  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - start_time)
                       .count();
  BOOST_LOG_TRIVIAL(info) << "Warm start cont " << container_id;
  BOOST_LOG_TRIVIAL(info) << "Warm start time "
                          << container_map_->GetContainerAppl(container_id)
                          << " " << exec_time;

  // Update status of the respective container
  // Try to gather a shared_ptr to the application. If not found --
  // application has crashed - ABORT.
  if (auto appl_ptr = appl_.lock()) {
    // Restart script has launched -- so, directly put into Running state
    appl_ptr->UpdateContainerStatus(container_id, ContainerStatus::kRunning);
    resp->set_usedmemory(appl_ptr->CurrentUsedMemory());
  } else {
    return Status(StatusCode::FAILED_PRECONDITION,
                  "Application not running on machine");
  }

  return Status::OK;
}

Status DedupServerImpl::Purge(ServerContext* ctxt, const Container* args,
                              MemoryResponse* resp) {
  // BOOST_LOG_TRIVIAL(trace) << "In Purge() function";
  int container_id = args->containerid();

  if (auto appl_ptr = appl_.lock()) {
    // Purge container
    appl_ptr->PurgeContainer(container_id);
    resp->set_usedmemory(appl_ptr->CurrentUsedMemory());
  } else {
    return Status(StatusCode::FAILED_PRECONDITION,
                  "Application not running on machine");
  }

  return Status::OK;
}

Status DedupServerImpl::Restore(ServerContext* ctxt, const Container* args,
                                MemoryResponse* resp) {
  if (ctxt->IsCancelled()) {
    return Status(StatusCode::CANCELLED,
                  "Deadline exceeded or Client cancelled, abandoning.");
  }

  int container_id = args->containerid();
  ContainerStatus status = container_map_->GetContainerStatus(container_id);

  if (status != kDedup) {
    BOOST_LOG_TRIVIAL(error)
        << "Container " << container_id << " not in dedup " << status;
    return Status(StatusCode::CANCELLED, "Container is not in dedup state.");
  }

  BOOST_LOG_TRIVIAL(trace) << "In Restore() function " << container_id;

  // Try to gather a shared_ptr t the application. If not found -- application
  // has crashed - ABORT.
  if (auto appl_ptr = appl_.lock()) {
    // NOTE: The following RestoreContainer call is blocking and other
    // operations from the controller will be in queue while this operation gets
    // completed.
    auto start_time = system_clock::now();
    int res = appl_ptr->RestoreContainer(container_id);

    auto exec_time = duration_cast<duration<double, std::milli>>(
                         system_clock::now() - start_time)
                         .count();
    BOOST_LOG_TRIVIAL(info)
        << "Dedup start time " << container_map_->GetContainerAppl(container_id)
        << " " << exec_time;

    // Set the used memory information back
    resp->set_usedmemory(appl_ptr->CurrentUsedMemory());

    // If successful, return OK else INVALID ARGUMENT
    if (res == 0) {
      return Status::OK;
    } else {
      return Status(StatusCode::INVALID_ARGUMENT, "Restore failed");
    }
  } else {
    return Status(StatusCode::FAILED_PRECONDITION,
                  "Application not running on machine");
  }
}

Status DedupServerImpl::Terminate(ServerContext* ctxt, const Ack* args,
                                  Ack* resp) {
  if (auto appl_ptr = appl_.lock()) {
    appl_ptr->Terminate();
  }

  return Status::OK;
}