// Run the Dedup Application on a single host machine. This is the entry point
// for the Dedup Application. CLI Arguments:
// 1: Machine ID for which the Dedup Application is running
// 2: Number of threads to utilize for the thread pool

#include "dedup_application.h"

#include <curl/curl.h>
#include <dirent.h>
#include <errno.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/asio/post.hpp>
#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "INIReader.h"
#include "controller.grpc.pb.h"
#include "main.grpc.pb.h"

using boost::asio::post;
using namespace boost::filesystem;
using dedup::Ack;
using dedup::BaseContainerResponse;
using dedup::Container;
using dedup::ContainerPages;
using dedup::DecisionResponse;
using dedup::MemoryArgs;
using dedup::StatusArgs;
using dedup::StatusArgs_Status;
using grpc::ClientContext;
using grpc::CreateCustomChannel;
using grpc::InsecureChannelCredentials;
using grpc::Server;
using grpc::ServerBuilder;
using std::ifstream;
using std::istringstream;
using std::shared_ptr;
using std::string;
using std::thread;
using std::vector;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::system_clock;
using std::chrono::time_point;

using PageStruct = dedup::ContainerPages_HashStruct;
using ApplDecision = dedup::DecisionResponse_Decision;

void DedupApplication::UpdateStatusOnController(int container_id) {
  ContainerStatus status = local_containers_.at(container_id)->status();

  // Update container status at controller
  StatusArgs args;
  args.set_containerid(container_id);
  if (status == kBase) {
    args.set_status(StatusArgs_Status::StatusArgs_Status_BASE);
  } else if (status == kDedup) {
    args.set_status(StatusArgs_Status::StatusArgs_Status_DEDUP);
  } else {
    args.set_status(StatusArgs_Status::StatusArgs_Status_WARM);
  }

  Ack resp;
  ClientContext ctx;
  auto rpc_status = stub_->UpdateStatus(&ctx, args, &resp);
  if (!rpc_status.ok()) {
    // Remove the container
    post(workers_, std::bind(&DedupApplication::LaunchContainerPurge, this,
                             container_id));
  }
}

void DedupApplication::RegisterBaseContainer(
    int container_id, shared_ptr<ContainerPages> container_pages) {
  // BOOST_LOG_TRIVIAL(trace) << "Sending call for registering base pages";

  // Send the container pages argument to the controller
  Ack resp;
  ClientContext ctx;

  auto start_time = system_clock::now();
  Status status = stub_->RegisterPages(&ctx, *container_pages.get(), &resp);
  auto end_time = system_clock::now();

  auto exec_time =
      duration_cast<duration<double, std::milli>>(end_time - start_time)
          .count();
  BOOST_LOG_TRIVIAL(info) << "Register base pages time " << container_id << " "
                          << exec_time;

  // TODO: Repeat the process if status is not as expected.
  if (status.ok()) {
    UpdateContainerStatus(container_id, kBase);

    // Update container status on controller
    UpdateStatusOnController(container_id);
  } else {
    BOOST_LOG_TRIVIAL(error)
        << status.error_code() << " " << status.error_message();
  }

  // Clear the shared_ptr
  container_pages->clear_payload();
}

void DedupApplication::GetBasePages(
    int container_id, shared_ptr<ContainerPages> container_pages) {
  // BOOST_LOG_TRIVIAL(trace)
  //     << "Sending container pages to the controller for dedup";

  // STEP 3: Send the container pages argument to the controller
  ClientContext ctx;
  auto resp = std::make_shared<BaseContainerResponse>();

  // NOTE: This is a blocking synchronous call to the controller
  auto start_time = system_clock::now();
  Status status =
      stub_->GetBaseContainers(&ctx, *container_pages.get(), resp.get());
  auto end_time = system_clock::now();

  auto exec_time =
      duration_cast<duration<double, std::milli>>(end_time - start_time)
          .count();
  BOOST_LOG_TRIVIAL(info) << "Get base containers time " << container_id << " "
                          << exec_time;

  // Invoke the MemoryManager to process the response and the deduplicated dump
  // TODO: Repeat the process if status is not as expected.
  if (status.ok()) {
    // STEP 4: Deduplicate dump
    post(workers_, std::bind(&MemoryManager::DeduplicateDump, manager_.get(),
                             local_containers_.at(container_id), resp));
  } else {
    BOOST_LOG_TRIVIAL(error) << status.error_code();
  }

  // Clear the shared_ptr
  container_pages->clear_payload();
}

int DedupApplication::WriteToCRIUPipe(string cname) {
  // Open a non blocking call  - to set some timeout
  int pipe_fd;
  int timeout = 500;  // In ms

  auto start_time = system_clock::now();
  while ((pipe_fd = open(pipe_name_.c_str(), O_WRONLY | O_NONBLOCK)) == -1 &&
         errno == ENXIO) {
    double time_since_write = duration_cast<duration<double, std::milli>>(
                                  system_clock::now() - start_time)
                                  .count();
    if (time_since_write > timeout) {
      return -1;
    }

    // Wait for a short while to allow the reading end to open.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    continue;
  }

  double time_since_write = duration_cast<duration<double, std::milli>>(
                                system_clock::now() - start_time)
                                .count();
  BOOST_LOG_TRIVIAL(trace) << "CRIU Return time: " << time_since_write;

  int num_tries = 0;
  int max_tries = 100;
  while (true) {
    int written_bytes = write(pipe_fd, cname.c_str(), sizeof(cname.c_str()));
    if (written_bytes == sizeof(cname.c_str())) {
      // BOOST_LOG_TRIVIAL(trace) << "Written completely.";
      break;
    } else {
      num_tries += 1;
      if (num_tries > max_tries) {
        break;
      }
    }
  }

  close(pipe_fd);
  return 0;
}

string DedupApplication::ReadFromCRIUPipe(bool dedup) {
  // Open call blocks until a write is opened on this named pipe
  int pipe_fd;
  if (dedup) {
    pipe_fd = open(pipe_name_.c_str(), O_RDONLY);
  } else {
    pipe_fd = open("/tmp/restore_fifo", O_RDONLY);
  }

  char readbuf[20];
  int num_tries = 0;
  int max_tries = 100;

  while (true) {
    int read_bytes = read(pipe_fd, readbuf, sizeof(readbuf));
    if (read_bytes > 0 && readbuf[read_bytes - 1] == '\0') {
      // BOOST_LOG_TRIVIAL(trace) << "Read completely.";
      break;
    } else {
      num_tries += 1;
      if (num_tries > max_tries) {
        BOOST_LOG_TRIVIAL(error)
            << "Read max tries reached " << read_bytes << " " << pipe_fd;
        break;
      }
    }
  }
  // readbuf[read_bytes] = '\0';
  close(pipe_fd);

  string ret_str(readbuf);
  return ret_str;
}

void DedupApplication::DeduplicateContainer(int container_id) {
  /* Implements the 4 steps of getting a patch file.
   * 1. Launch the checkpoint operation and get the memory dumps from CRIU.
   * 2. Read the memory dumps and construct the object of chunk hashes.
   * 3. Send to controller and get the base page addresses.
   * 4. Make remote reads and compute patches to deduplicate the memory state.
   */
  auto curr_time = system_clock::now();

  // Restart the container before making checkpoint.
  DockerSingleOperation(container_id, "unpause", 204);

  // STEP 1
  GetContainerCheckpoint(container_id, true);

  // STEP 2
  // Compute memory hashes and proceed with deduplication
  auto container_pages = ComputeMemoryHashes(container_id, false);

  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - curr_time)
                       .count();

  BOOST_LOG_TRIVIAL(info) << "Dedup script time " << container_id << " "
                          << exec_time;

  // Set checkpointed now.
  local_containers_.at(container_id)->set_checkpointed();

  // STEP 3 and 4
  post(workers_, std::bind(&DedupApplication::GetBasePages, this, container_id,
                           container_pages));
}

void DedupApplication::FinishDeduplicate(int container_id) {
  // Clear file data in the container and m-unmap the read files
  // Remove the files from the tmpfs directory as well.
  local_containers_.at(container_id)->ClearFileData();

  // Launch script to remove pages-* files from the dump [if any]
  LaunchScript("remove_pages", std::to_string(container_id));

  // Update container status on local data structure
  UpdateContainerStatus(container_id, kDedup);

  // Update status on controller
  UpdateStatusOnController(container_id);
}

void DedupApplication::LaunchContainerPause(int container_id) {
  // First update status, then actually make change in Docker
  UpdateContainerStatus(container_id, kWarm);

  int result = DockerSingleOperation(container_id, "pause", 204);

  // Also update availability on controller - only if result is 0
  if (result == 0) {
    UpdateStatusOnController(container_id);
  }
}

void DedupApplication::LaunchContainerForceRemove(int container_id) {
  BOOST_LOG_TRIVIAL(info) << "Force removing container " << container_id << " "
                          << local_containers_.at(container_id)->status();

  int ret = DockerSingleOperation(container_id, "remove?force=true", 204);

  // Remove the dump location directory.
  LaunchScript("remove_dump", std::to_string(container_id));

  // Remove the container from the map as well now
  std::lock_guard<std::mutex> guard(container_map_mutex_);
  // Clear the file data
  local_containers_.erase(container_id);
  BOOST_LOG_TRIVIAL(info) << "Force removed container " << container_id;
}

void DedupApplication::LaunchContainerPurge(int container_id) {
  BOOST_LOG_TRIVIAL(info) << "Purging container " << container_id << " "
                          << local_containers_.at(container_id)->status();

  auto container_ptr = local_containers_.at(container_id);

  // Clear the file data
  container_ptr->ClearFileData();

  BOOST_LOG_TRIVIAL(trace) << "Cleared file data " << container_id;

  // The CRIU restore-paused state is in two processes.
  int restore_pid = container_ptr->restore_pid();
  int cloned_criu_pid = container_ptr->cloned_criu_pid();
  if (restore_pid != -1) {
    // Kill the restore process (if any)
    int res = kill(restore_pid, SIGKILL);
    res = kill(cloned_criu_pid, SIGKILL);
    if (res != 0) {
      BOOST_LOG_TRIVIAL(error) << "Kill pid failed";
    }
  }

  BOOST_LOG_TRIVIAL(trace) << "Killed restore processes " << container_id;

  // Stop the container and then remove it
  ContainerStatus prev_status = container_ptr->prev_status();
  // Status kDedup implies a failed Dedup container.
  ContainerStatus status = container_ptr->status();
  // if (status != kDedup) {
  //   DockerSingleOperation(container_id, "stop", 204);
  // }
  // BOOST_LOG_TRIVIAL(trace) << "Stopped if needed " << container_id;

  try {
    int ret = DockerSingleOperation(container_id, "remove?force=true", 204);
    if (ret == -1) {
      // Try again
      int ret = DockerSingleOperation(container_id, "remove", 204);
    }

    BOOST_LOG_TRIVIAL(trace)
        << "Removed container from docker " << container_id;

    // Remove the dump location directory.
    LaunchScript("remove_dump", std::to_string(container_id));

    // Check if the dump location still exists
    DIR* dir = opendir(container_ptr->dump_location().c_str());
    if (dir) {
      BOOST_LOG_TRIVIAL(info) << "DELETE unsuccessful";
    } else if (errno == ENOENT) {
      BOOST_LOG_TRIVIAL(info) << "DIR DELETED " << container_id;
    }
    closedir(dir);

    BOOST_LOG_TRIVIAL(trace) << "Remove dump " << container_id;

    // Purge the container now
    std::lock_guard<std::mutex> guard(container_map_mutex_);
    local_containers_.erase(container_id);
    BOOST_LOG_TRIVIAL(info) << "Purged container " << container_id;
  } catch (...) {
    // Before anything, set container status to be PURGE
    UpdateContainerStatus(container_id, kPurge);
    BOOST_LOG_TRIVIAL(info)
        << "Error while purging " << container_id << ". Ignoring.";
  }
}

void DedupApplication::GetContainerCheckpoint(int container_id, bool dedup) {
  auto lock_acq_time = system_clock::now();
  string log_term = dedup ? "CRIU DUMP LOCK " : "CRIU BASE LOCK ";

  // Create the dump location of the container
  mkdir(local_containers_.at(container_id)->dump_location().c_str(), 0755);

  auto curr_time = system_clock::now();
  // Demarcate critical section so that the lock is released as soon as possible
  {
    BOOST_LOG_TRIVIAL(info) << "WAIT " << log_term << container_id;
    std::lock_guard<std::mutex> guard(dump_mutex_);
    lock_acq_time = system_clock::now();
    BOOST_LOG_TRIVIAL(info) << "ACQ " << log_term << container_id;

    // Start the script in a separate thread. Exit the container if DEDUP.
    thread docker_api(DockerCreateCheckpoint, container_id, dedup);

    // Write the argument to CRIU on the pipe.
    string cname = "cont" + std::to_string(container_id);
    WriteToCRIUPipe(cname);

    docker_api.join();
    BOOST_LOG_TRIVIAL(info) << "REL " << log_term << container_id;
  }

  auto lock_time =
      duration_cast<duration<double, std::milli>>(lock_acq_time - curr_time)
          .count();
  if (dedup) {
    BOOST_LOG_TRIVIAL(info)
        << "Dump Lock Acq time " << container_id << " " << lock_time;
  } else {
    BOOST_LOG_TRIVIAL(info)
        << "Base Lock Acq time " << container_id << " " << lock_time;
  }
}

void DedupApplication::LaunchContainerBase(int container_id) {
  bool checkpointed = local_containers_.at(container_id)->checkpointed();

  // Needed for PrevOp after completion of an execution of a dedup container.
  if (checkpointed) {
    // If the checkpoint is already done, then only launch a pause script
    int result = DockerSingleOperation(container_id, "pause", 204);

    // Once paused, simply update state - locally and on controller
    if (result == 0) {
      UpdateContainerStatus(container_id, kBase);
      UpdateStatusOnController(container_id);
    } else {
      BOOST_LOG_TRIVIAL(error)
          << "Base container pause op failed " << container_id;
    }

    return;
  }

  // If the checkpoint is not yet done, then the container was in a WARM state
  // and controller decision gave BASE. Restart the container before checkpoint.
  int result = DockerSingleOperation(container_id, "unpause", 204);
  if (result != 0) {
    BOOST_LOG_TRIVIAL(error)
        << "Base container restart failed " << container_id;
  }

  /*
   * Base operation is completed in three steps:
   * 1. Launch the checkpoint operation and write container id on the CRIU pipe.
   * 2. Read the memory dumps and construct the object of chunk hashes
   * 3. Send them to register at the controller and update container status.
   */

  auto curr_time = system_clock::now();

  // STEP 1
  GetContainerCheckpoint(container_id, false);

  // Once the dump is obtained - pause the container.
  DockerSingleOperation(container_id, "pause", 204);

  // Remove unnecessary dumps in the dump location = launch op using system()
  LaunchScript("remove_nonpages", std::to_string(container_id));

  // STEP 2
  // Update local container status and launch further operations on threadpool
  // Compute memory hashes and send to the controller for registering
  auto container_pages = ComputeMemoryHashes(container_id, true);

  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - curr_time)
                       .count();

  BOOST_LOG_TRIVIAL(info) << "Base script time " << container_id << " "
                          << exec_time;
  // BOOST_LOG_TRIVIAL(trace)
  //     << "Number of pages: " << container_pages->payload_size();

  // STEP 3
  post(workers_, std::bind(&DedupApplication::RegisterBaseContainer, this,
                           container_id, container_pages));
}

void DedupApplication::LaunchContainerDedup(int container_id, bool stop) {
  /*
   * Dedup operation is completed in four steps:
   * 0. Get the patch file to store container in Deduplicated state.
   * 1. Start the Restore process.
   * 2. Pause and get the CRIU process pid(s)
   * 3. Remove the original dump files.
   *
   * The patch file may be already present if the container was dedup-ed at an
   * earlier point in time. For the first time, the process to obtain the patch
   * file is in \ref DeduplicateContainer().
   */

  bool checkpointed = local_containers_.at(container_id)->checkpointed();
  if (!checkpointed) {
    // STEP 0: Container being deduplicated for the first time - compute the
    // patch file first. At completion of deduplication, this function will be
    // called again - with `checkpointed' as `true'.
    post(workers_, std::bind(&DedupApplication::DeduplicateContainer, this,
                             container_id));
    return;
  }

  if (stop) {
    // Before restore-pause state, stop container. For first time - checkpoint
    // will already stop the container.
    DockerSingleOperation(container_id, "stop", 204);
  }

  auto curr_time = system_clock::now();
  auto lock_acq_time = system_clock::now();
  string cname = "cont" + std::to_string(container_id);
  bool success = false;
  bool flag = true;

  // STEP 1
  // Demarcate critical section for CRIU lock
  {
    BOOST_LOG_TRIVIAL(info) << "WAIT CRIU DEDUP LOCK " << container_id;
    std::lock_guard<std::mutex> guard(dump_mutex_);
    lock_acq_time = system_clock::now();
    BOOST_LOG_TRIVIAL(info) << "ACQ CRIU DEDUP LOCK " << container_id;

    // Start the restore pause procedure in a detached thread
    thread restore_api(DockerSingleOperation, container_id,
                       "start?checkpoint=cp" + std::to_string(container_id),
                       204);
    restore_api.detach();

    // Write dump location for the restore process
    int ret = WriteToCRIUPipe(cname);
    if (ret != -1) {
      // STEP 2: Read the restore process PID.
      string pid_str = ReadFromCRIUPipe(true);
      std::stringstream pid_ss(pid_str);

      int cloned_criu_pid = -1, root_criu_pid = -1;
      string pid;

      // Try to decode the read output. If it does not work: then there's an
      // issue with CRIU.
      try {
        while (pid_ss >> pid) {
          if (cloned_criu_pid == -1) {
            cloned_criu_pid = std::stoi(pid);
          } else {
            root_criu_pid = std::stoi(pid);
          }
        }
      } catch (const std::exception& e) {
        flag = false;
        BOOST_LOG_TRIVIAL(error)
            << "Received invalid pid from CRIU: " << pid_str;
      }

      if (cloned_criu_pid == -1 || root_criu_pid == -1) {
        flag = false;
        BOOST_LOG_TRIVIAL(error)
            << "Received invalid pid from CRIU: " << pid_str;
      }

      success = true && flag;
      local_containers_.at(container_id)->set_restore_pid(root_criu_pid);
      local_containers_.at(container_id)->set_cloned_criu_pid(cloned_criu_pid);
      // BOOST_LOG_TRIVIAL(trace) << "Received CRIU PID: " << pid;
    } else {
      BOOST_LOG_TRIVIAL(error) << "Write to restore timed out " << container_id;
    }

    BOOST_LOG_TRIVIAL(info) << "REL CRIU DEDUP LOCK " << container_id;
  }

  auto lock_time =
      duration_cast<duration<double, std::milli>>(lock_acq_time - curr_time)
          .count();
  BOOST_LOG_TRIVIAL(info) << "Dedup Lock Acq time " << container_id << " "
                          << lock_time;

  // STEP 3: Cleanup and update status
  if (success) {
    FinishDeduplicate(container_id);
  } else {
    // Blacklist container
    BOOST_LOG_TRIVIAL(trace) << "Blacklist " << container_id;
    Container container;
    container.set_containerid(container_id);

    Ack ack;
    ClientContext ctx;

    Status status = stub_->Blacklist(&ctx, container, &ack);

    if (flag) {
      // Revert back to warm state - start container and then pause it.
      // This container is not suitable for dedup.
      BOOST_LOG_TRIVIAL(info)
          << "Container deduplication failed. Fall back to warm.";
      DockerSingleOperation(container_id, "start", 204);
      LaunchContainerPause(container_id);
    } else {
      // Purge the container - container should maintain a dummy status on
      // controller
      LaunchContainerPurge(container_id);
    }
  }
}

void DedupApplication::LaunchContainerDedupNoPause(int container_id,
                                                   bool stop) {
  /*
   * Dedup operation is completed in two steps:
   * 0. Get the patch file to store container in Deduplicated state.
   * 1. Remove the original dump files.
   *
   * The patch file may be already present if the container was dedup-ed at an
   * earlier point in time. For the first time, the process to obtain the patch
   * file is in \ref DeduplicateContainer().
   */

  bool checkpointed = local_containers_.at(container_id)->checkpointed();
  if (!checkpointed) {
    // STEP 0: Container being deduplicated for the first time - compute the
    // patch file first. At completion of deduplication, this function will be
    // called again - with `checkpointed' as `true'.
    post(workers_, std::bind(&DedupApplication::DeduplicateContainer, this,
                             container_id));
    return;
  }

  bool success = true;
  if (stop) {
    // Before restore-pause state, stop container. For first time - checkpoint
    // will already stop the container.
    int res = DockerSingleOperation(container_id, "stop", 204);
    success = res != -1;
  }

  // STEP 1: Cleanup and update status
  if (success) {
    FinishDeduplicate(container_id);
  } else {
    // Purge the container - container should maintain a dummy status on
    // controller
    LaunchContainerPurge(container_id);
  }
}

void DedupApplication::LaunchContainerPrevOp(int container_id) {
  auto container_ptr = ContainerAtId(container_id);
  if (container_ptr == nullptr) {
    return;
  }

  auto prev_status = container_ptr->prev_status();
  auto curr_time = system_clock::now();
  auto last_mod = duration_cast<duration<double, std::milli>>(
                      curr_time - container_ptr->last_modified())
                      .count();

  switch (prev_status) {
    case kWarm:
      // Simply continue keeping the container warm
      LaunchContainerPause(container_id);
      break;

    case kBase:
      // Launch Container Base Op
      LaunchContainerBase(container_id);
      break;

    case kDedup:
      // Keep the container WARM as we do not want to pay expensive costs for
      // dedup repeatedly.
      LaunchContainerPause(container_id);
      break;

    default:
      break;
  }
}

void DedupApplication::GetDecision(int cid) {
  // Construct the Container object to pass as argument
  Container container;
  container.set_containerid(cid);

  BOOST_LOG_TRIVIAL(trace) << "Fetch decision for " << cid;

  DecisionResponse resp;
  ClientContext ctx;

  Status status = stub_->GetDecision(&ctx, container, &resp);

  // Check if status is OK and if the container is still in a Dummy state
  if (!status.ok()) {
    return;
  }

  // Compare with the previous status to decide whether to launch docker cmd.
  ContainerStatus prev_status = local_containers_.at(cid)->prev_status();
  bool no_op = false;

  // NOTE: Spawn tasks on thread pool to avoid having large jobs.
  auto decision = resp.decision();

  BOOST_LOG_TRIVIAL(trace) << "Got decision for " << cid << " " << decision;

  switch (decision) {
    case ApplDecision::DecisionResponse_Decision_BASE:
      if (prev_status == kBase) {
        no_op = true;
      } else {
        // Use as Base container - get page hashes
        post(workers_,
             std::bind(&DedupApplication::LaunchContainerBase, this, cid));
      }
      break;

    case ApplDecision::DecisionResponse_Decision_WARM:
      if (prev_status == kWarm) {
        no_op = true;
      } else {
        // If the container was previously deduped - first start the restore and
        // then put in warm state.
        if (prev_status == kDedup) {
          RestoreContainer(cid);
        }
        post(workers_,
             std::bind(&DedupApplication::LaunchContainerPause, this, cid));
      }
      break;

    case ApplDecision::DecisionResponse_Decision_DEDUP:
      if (prev_status == kDedup) {
        no_op = true;
      } else {
        // Get page hashes and send to controller
        // Container already paused - stop before starting from checkpoint again
        if (no_pause_) {
          post(workers_,
               std::bind(&DedupApplication::LaunchContainerDedupNoPause, this,
                         cid, true));
        } else {
          post(workers_, std::bind(&DedupApplication::LaunchContainerDedup,
                                   this, cid, true));
        }
      }
      break;

    case ApplDecision::DecisionResponse_Decision_PURGE:
      // Launch Purge Script and remove container from local container index.
      post(workers_,
           std::bind(&DedupApplication::LaunchContainerPurge, this, cid));
      break;

    default:
      break;
  }

  // The container decision is exactly what the container is already in.
  // Hence, do not need to make any changes. Happens when a WARM/BASE/DEDUP
  // container remains idle and lands up with the exact same decision.
  if (no_op) {
    UpdateContainerStatus(cid, prev_status);
    UpdateStatusOnController(cid);
  }
}

float DedupApplication::CurrentUsedMemory() {
  float used_mem = 0.0;
  unsigned long total_mem = 0;
  unsigned long avail_mem = 0;

  string token;
  std::ifstream file("/proc/meminfo");
  bool err = false;
  while (file >> token) {
    if (token == "MemTotal:") {
      if (!(file >> total_mem)) {
        err = true;
      }
    } else if (token == "MemAvailable:") {
      if (!(file >> avail_mem)) {
        err = true;
      }
    }
    if (avail_mem != 0 && total_mem != 0) {
      break;
    }
  }

  used_mem = (float)(total_mem - avail_mem) / 1024;  // In MB
  double bote_mem_purged = 0.0;
  double bote_mem_dedup = 0.0;

  // Calculate and add the patch files as well.
  unsigned long checkpoint_mem = 0;
  for (auto container : local_containers_) {
    path p(container.second->dump_location() + "/patch-file.img");
    if (exists(p)) {
      if (is_regular_file(p)) {
        checkpoint_mem += file_size(p);
      }
    }

    // Back of the envelope calculation -- Subtract the memory for the
    // containers that are marked purged
    if (container.second->status() == kPurge) {
      bote_mem_purged += applications_.at(container.second->appl())->memory_;
    }

    // Back of the envelope calculation -- Subtract the memory overhead for
    // deduplicated containers
    if (container.second->status() == kDedup) {
      bote_mem_dedup += 10;
    }
  }

  float checkpoint_size = (float)checkpoint_mem / (1024 * 1024);  // In Mb
  used_mem += checkpoint_size;
  used_mem -= bote_mem_purged;
  used_mem -= bote_mem_dedup;
  BOOST_LOG_TRIVIAL(info) << "Purged containers memory: " << bote_mem_purged;

  // Update last sent param
  last_sent_ = system_clock::now();

  return (used_mem - initial_mem_);
}

void DedupApplication::Daemon() {
  BOOST_LOG_TRIVIAL(trace) << "Started Daemon on Client";

  auto last_update_time = system_clock::now();
  auto start_time = system_clock::now();
  initial_mem_ = CurrentUsedMemory();
  BOOST_LOG_TRIVIAL(info) << "Initial mem usage: " << initial_mem_;

  while (true) {
    // Runs continuously and launches (heavy) functions using a thread pool.
    auto curr_time = system_clock::now();

    int breakdown[6] = {0};  // Used only for motivation exps
    {
      // Acquire lock before iterating over the container map.
      std::lock_guard<std::mutex> guard(container_map_mutex_);
      for (auto& container_pair : local_containers_) {
        // Check state machine logic
        int cid = container_pair.first;
        auto container_ptr = container_pair.second;
        ContainerStatus status = container_ptr->status();
        auto appl_conf = applications_.at(container_ptr->appl());
        auto last_mod = duration_cast<duration<double, std::milli>>(
                            curr_time - container_ptr->last_modified())
                            .count();

        double exec_time = 0.0;

        /**
         * NOTE: The daemon only need to check for non-startup cases, as those
         * are time bound by the daemon. The startup cases are bound by actual
         * run time, which will be handled by the respective thread.
         */
        switch (status) {
          case kWarm:
            // Check if need to launch GetDecision call
            breakdown[kWarm] += 1;
            if (!adaptive_) {
              if (last_mod > 1000 * idle_time_ ||
                  last_mod > 1000 * appl_conf->keep_alive_) {
                // Fetch decision for the container and move accordingly.
                // BOOST_LOG_TRIVIAL(trace) << "Fetch decision for warm " << cid;
                UpdateContainerStatus(cid, kDummy);
                post(workers_,
                     std::bind(&DedupApplication::GetDecision, this, cid));
              }
            } else {
              auto time_since_start =
                  duration_cast<duration<double, std::milli>>(curr_time -
                                                              start_time)
                      .count();
              bool half_time = time_since_start > 1800;

              // For adaptive run as per the different timeouts for different
              // applications
              string appl = container_ptr->appl();
              int keepalive_times[10][2] = {
                  {102, 294}, {324, 522}, {60, 230},  {230, 288}, {198, 132},
                  {199, 300}, {462, 432}, {348, 132}, {30, 60},   {294, 84},
              };
              int index = half_time ? 1 : 0;
              int time = keepalive_times[std::stoi(appl)][index];
              if (last_mod > 1000 * time) {
                // Fetch decision for the container and move accordingly.
                UpdateContainerStatus(cid, kDummy);
                post(workers_,
                     std::bind(&DedupApplication::GetDecision, this, cid));
              }
            }
            break;

          case kBase:
            breakdown[kBase] += 1;
            // Base containers never request decisions
            if (last_mod > 6 * 1000 * appl_conf->keep_alive_) {
              // Launch GetDecision on thread pool
              // BOOST_LOG_TRIVIAL(trace) << "Fetch decision for base " << cid;
              UpdateContainerStatus(cid, kDummy);
              post(workers_,
                   std::bind(&DedupApplication::GetDecision, this, cid));
            }
            break;

          case kDedup:
            breakdown[kDedup] += 1;
            if (last_mod > 1000 * idle_time_) {
              // Launch GetDecision on thread pool
              // BOOST_LOG_TRIVIAL(trace) << "Fetch decision for dedup " << cid;
              UpdateContainerStatus(cid, kDummy);
              post(workers_,
                   std::bind(&DedupApplication::GetDecision, this, cid));
            }
            break;

          case kRunning:
            breakdown[kRunning] += 1;
            exec_time = appl_conf->exec_time_;

            // Emulate checkpoints
            // if (container_ptr->first_spawned()) {
            //   string appl = container_ptr->appl();
            //   if (appl == "3") {
            //     exec_time = 100;
            //   } else if (appl == "4") {
            //     exec_time = 1500;
            //   } else if (appl == "5") {
            //     exec_time = 100;
            //   }
            // }

            if (last_mod > exec_time) {
              // Update to Dummy so that another operation isn't fired in next
              // iteration of the daemon
              UpdateContainerStatus(cid, kDummy);

              if (container_ptr->first_spawned()) {
                container_ptr->set_first_spawned();
              }

              // Launch PrevOp on thread pool
              auto prev_op = std::bind(&DedupApplication::LaunchContainerPrevOp,
                                       this, cid);
              post(workers_, prev_op);
            }
            break;

          case kPurge:
            breakdown[kPurge] += 1;
            // NOTE: Last modified will be changed when the container was first
            // put in the PURGE state. Purge after one minute.
            if (last_mod > 60 * 1000) {
              // BOOST_LOG_TRIVIAL(trace) << "Try re-purging container " << cid;
              UpdateContainerStatus(cid, kDummy);
              post(workers_,
                   std::bind(&DedupApplication::LaunchContainerForceRemove,
                             this, cid));
            }
            break;

          default:
            // Simply mark the container PURGE if last modified keep_alive time
            // ago.
            breakdown[kDummy] += 1;
            // if (last_mod > 1000) {
            //   if (breakdown[kDummy] < 5) {
            //     BOOST_LOG_TRIVIAL(trace) << "Previous state of " << cid << " "
            //                              << container_ptr->prev_status();
            //   }
            // }
            if (last_mod > 1000 * appl_conf->keep_alive_) {
              UpdateContainerStatus(cid, kPurge);
            }
            break;
        }
      }
    }

    // Calculate and send the used memory stats to the controller: every 10 sec
    auto last_mod =
        duration_cast<duration<double, std::milli>>(curr_time - last_sent_)
            .count();
    if (last_mod > 10 * 1000) {
      float used_mem = CurrentUsedMemory();

      Ack resp;
      ClientContext ctx;
      MemoryArgs args;
      args.set_machine(machine_id_);
      args.set_usedmemory(used_mem);
      stub_->UpdateAvailableMemory(&ctx, args, &resp);
    }

    // Only for motivation exps:
    last_mod = duration_cast<duration<double, std::milli>>(curr_time -
                                                           last_update_time)
                   .count();
    if (last_mod > 1000) {
      last_update_time = system_clock::now();
      BOOST_LOG_TRIVIAL(info)
          << "Containers: W " << breakdown[kWarm] << " B " << breakdown[kBase]
          << " D " << breakdown[kDedup] << " R " << breakdown[kRunning] << " P "
          << breakdown[kPurge] << " Dummy " << breakdown[kDummy] << " "
          << local_containers_.size();
    }

    // Sleep for a while before iterating over the container map again.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void DedupApplication::RunServer(string address) {
  std::string server_address(address);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(service_.get());
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  BOOST_LOG_TRIVIAL(info) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void DedupApplication::PurgeContainer(int container_id) {
  // Purge container if it is already in a warm state - otherwise the purge op
  // is already ongoing.
  ContainerStatus status = local_containers_.at(container_id)->status();
  if (status != kDummy) {
    // Mark container Dummy and launch on a worker thread -- don't halt the
    // controller.
    BOOST_LOG_TRIVIAL(trace) << "Purged container " << container_id;
    UpdateContainerStatus(container_id, kDummy);
    post(workers_, std::bind(&DedupApplication::LaunchContainerPurge, this,
                             container_id));
  }
}

int DedupApplication::RestoreContainer(int container_id) {
  if (no_pause_) {
    return RestoreContainerNoPause(container_id);
  }

  /*
   * The Restore Procedure happens in three steps:
   * 1. Use MemoryManager to restore the original pages of the container
   * 2. Resume the paused CRIU process
   * 3. Cleanup by removing the generated pages in Step 1
   */

  // Update container status to be DUMMY so that it does not start GetDecision.
  BOOST_LOG_TRIVIAL(trace) << "Restore container " << container_id;
  UpdateContainerStatus(container_id, kDummy);

  // STEP 1: Use the MemoryManager to restore dumps into image files that CRIU
  // can understand
  auto curr_time = system_clock::now();
  manager_->RestoreDump(local_containers_.at(container_id));
  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - curr_time)
                       .count();
  BOOST_LOG_TRIVIAL(info) << "ML restore time " << container_id << " "
                          << exec_time;

  curr_time = system_clock::now();

  auto lock_acq_time = system_clock::now();
  // STEP 2: Pass the restore ID and then wait for completion from CRIU
  // process
  {
    BOOST_LOG_TRIVIAL(info) << "WAIT RESTORE CRIU LOCK " << container_id;
    std::lock_guard<std::mutex> guard(restore_mutex_);
    lock_acq_time = system_clock::now();
    BOOST_LOG_TRIVIAL(info) << "ACQ RESTORE CRIU LOCK " << container_id;

    // Call the shell script to restore, with the container argument
    int pid = local_containers_.at(container_id)->restore_pid();
    kill(pid, SIGUSR1);
    // waitpid(pid, NULL, 0);

    string result = ReadFromCRIUPipe(false);
    // BOOST_LOG_TRIVIAL(info) << "Received from CRIU: " << result;
    BOOST_LOG_TRIVIAL(info) << "REL RESTORE CRIU LOCK " << container_id;
  }

  auto lock_time =
      duration_cast<duration<double, std::milli>>(lock_acq_time - curr_time)
          .count();
  exec_time = duration_cast<duration<double, std::milli>>(system_clock::now() -
                                                          curr_time)
                  .count();
  BOOST_LOG_TRIVIAL(info) << "Restore Lock Acq time " << container_id << " "
                          << lock_time;
  BOOST_LOG_TRIVIAL(info) << "Restore script time " << container_id << " "
                          << exec_time;

  // STEP 3: At this point, container is restored and hence, dump files can be
  // removed.
  LaunchScript("remove_pages", std::to_string(container_id));

  // Update container status to RUNNING
  UpdateContainerStatus(container_id, kRunning);

  return 0;
}

int DedupApplication::RestoreContainerNoPause(int container_id) {
  /*
   * The Restore Procedure happens in three steps:
   * 1. Use MemoryManager to restore the original pages of the container
   * 2. Start the container from the checkpoint
   * 3. Cleanup by removing the generated pages in Step 1
   */

  // STEP 1: Use the MemoryManager to restore dumps into image files that CRIU
  // can understand
  auto curr_time = system_clock::now();
  manager_->RestoreDump(local_containers_.at(container_id));
  auto exec_time = duration_cast<duration<double, std::milli>>(
                       system_clock::now() - curr_time)
                       .count();
  BOOST_LOG_TRIVIAL(info) << "ML restore time " << container_id << " "
                          << exec_time;

  curr_time = system_clock::now();
  auto lock_acq_time = system_clock::now();
  string cname = "cont" + std::to_string(container_id);

  // STEP 2
  // Demarcate critical section for CRIU lock
  {
    BOOST_LOG_TRIVIAL(info) << "WAIT CRIU RESTORE LOCK " << container_id;
    std::lock_guard<std::mutex> guard(dump_mutex_);
    lock_acq_time = system_clock::now();
    BOOST_LOG_TRIVIAL(info) << "ACQ CRIU RESTORE LOCK " << container_id;

    // Start the restore pause procedure in a detached thread
    thread restore_api(DockerSingleOperation, container_id,
                       "start?checkpoint=cp" + std::to_string(container_id),
                       204);

    // Write dump location for the restore process
    int ret = WriteToCRIUPipe(cname);
    if (ret == -1) {
      BOOST_LOG_TRIVIAL(error) << "Write to restore timed out " << container_id;
    }

    restore_api.join();
    BOOST_LOG_TRIVIAL(info) << "REL CRIU RESTORE LOCK " << container_id;
  }

  auto lock_time =
      duration_cast<duration<double, std::milli>>(lock_acq_time - curr_time)
          .count();
  exec_time = duration_cast<duration<double, std::milli>>(system_clock::now() -
                                                          curr_time)
                  .count();
  BOOST_LOG_TRIVIAL(info) << "Restore Lock Acq time " << container_id << " "
                          << lock_time;
  BOOST_LOG_TRIVIAL(info) << "Restore script time " << container_id << " "
                          << exec_time;

  // STEP 3: At this point, container is restored and hence, dump files can be
  // removed.
  LaunchScript("remove_pages", std::to_string(container_id));

  // Update container status to RUNNING
  UpdateContainerStatus(container_id, kRunning);

  return 0;
}

void DedupApplication::AddContainer(int container_id, string appl) {
  // Create a shared_ptr to a new ContainerInfo instance and add to
  // local_containers_
  std::lock_guard<std::mutex> guard(container_map_mutex_);
  auto c_info = std::make_shared<ContainerInfo>(container_id, appl);
  local_containers_.insert(std::make_pair(container_id, c_info));
  container_map_->AddContainer(container_id, c_info);
}

void DedupApplication::UpdateContainerStatus(int container_id,
                                             ContainerStatus status) {
  // BOOST_LOG_TRIVIAL(trace) << "Status " << container_id << " " << status;
  local_containers_.at(container_id)->UpdateStatus(status);
}

void DedupApplication::ReadConfig() {
  INIReader reader("config/agent.ini");

  chunks_per_page_ = reader.GetInteger("parameters", "chunksperpage", 2);
  idle_time_ = reader.GetInteger("parameters", "idletime", 60);
  int nopause = reader.GetInteger("parameters", "nopause", 0);
  no_pause_ = nopause == 0 ? false : true;
  int adaptive = reader.GetInteger("parameters", "adaptive", 0);
  adaptive_ = adaptive == 0 ? false : true;
  if (adaptive_) {
    BOOST_LOG_TRIVIAL(info) << "Using the adaptive keep-alive policy";
  }

  int num_appl = reader.GetInteger("configuration", "numappl", 1);
  for (int i = 0; i < num_appl; i++) {
    string conf_str =
        reader.Get("configuration", "appl" + std::to_string(i), "");

    string keep_alive, exec_time, memory;
    istringstream conf_ss(conf_str);
    conf_ss >> keep_alive;
    conf_ss >> exec_time;
    conf_ss >> memory;
    applications_.insert(std::make_pair(
        std::to_string(i),
        std::make_shared<ApplConfiguration>(
            std::stoi(keep_alive), std::stoi(exec_time), std::stod(memory))));
  }

  int thres = reader.GetInteger("parameters", "patchthreshold", 2048);
  manager_->set_patch_threshold(thres);
  manager_->set_no_pause(no_pause_);
}

void DedupApplication::Terminate() {
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  curl_global_cleanup();
  exit(0);
}

std::shared_ptr<ContainerPages> DedupApplication::ComputeMemoryHashes(
    int cid, bool flag) {
  // Read the CRIU dumps to get page hashes
  auto cpages = std::make_shared<ContainerPages>();
  cpages->set_machineid(machine_id_);
  cpages->set_containerid(cid);

  // Get the number of dump files first
  DIR* dp;
  struct dirent* dirp;
  int num_files = 0;

  string dump_location = local_containers_.at(cid)->dump_location();
  if ((dp = opendir(dump_location.c_str())) == NULL) {
    BOOST_LOG_TRIVIAL(error) << "Could not open dump location";
  }

  while ((dirp = readdir(dp)) != NULL) {
    std::string fname = dirp->d_name;
    if (fname.find("pages-") != std::string::npos) num_files += 1;
  }

  // Close the directory
  closedir(dp);
  local_containers_.at(cid)->set_num_dump_files(num_files);

  // Null Fingerprint
  string null_fp = "c8d7d0ef0eedfa82d2ea1aa592845b9a6d4b02b7";

  // Statistics
  int pages_with_zero_fp = 0;

  for (int i = 1; i <= num_files; i++) {
    string file_name = local_containers_.at(cid)->dump_location() + "/pages-" +
                       std::to_string(i) + ".img";
    int fd = open(file_name.c_str(), O_RDWR);
    if (fd == -1) {
      BOOST_LOG_TRIVIAL(error) << "Could not open dump file for reading";
      return std::make_shared<ContainerPages>();
    }

    // Get length of the file
    struct stat st;
    stat(file_name.c_str(), &st);
    int length = st.st_size;
    int num_pages = length / PAGE_SIZE;

    // Map all pages together using mmap syscall
    char* file_data = reinterpret_cast<char*>(
        mmap(0, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    // Hold on to the file data object
    local_containers_.at(cid)->AddFileData(file_data);

    // Set the number of pages for this dump file
    local_containers_.at(cid)->AddDumpFilePages(num_pages);

    // Register the entire file as a single memory region with RDMA
    int mr_ID = -1;
    if (flag) {
      mr_ID = manager_->RegisterMemoryRegion((void*)file_data, length);
      if (mr_ID == -1) {
        BOOST_LOG_TRIVIAL(error)
            << "Could not register base pages successfully";
      }
    }

    int ipage = 0;
    while (ipage < num_pages) {
      // Add to the ContainerPages struct
      PageStruct* page_struct = cpages->add_payload();
      int page_offset = ipage * PAGE_SIZE;

      if (flag) {
        // Set MemoryRegionID in PageStruct
        page_struct->set_addr(page_offset);
        page_struct->set_mrid(mr_ID);
      } else {
        // For dedup containers, save the file_id as the MemoryRegionID
        int page_id = cpages->payload_size() - 1;  // Use zero indexed pages

        // NOTE: For Dedup containers, addr is set as an obfuscated page_id,
        // which is assigned to pages in an increasing order.
        page_struct->set_addr(page_id);
        page_struct->set_mrid(mr_ID);
      }

      // Use a non-null chunk per segment
      vector<string> fingerprints =
          ValueSampledFingerprint(&file_data[page_offset], chunks_per_page_);

      bool all_zero = true;
      for (string fp : fingerprints) {
        page_struct->add_hashes(fp);
        if (fp != null_fp) all_zero = false;
      }

      if (all_zero) {
        pages_with_zero_fp += 1;
      }
      ipage += 1;
    }

    // Close the fd
    close(fd);
  }

  // Print statistics
  BOOST_LOG_TRIVIAL(info) << "Zero FP Pages " << cid << " "
                          << pages_with_zero_fp;

  return cpages;
}

// Wrapper to run server from a thread
void StartServer(shared_ptr<DedupApplication> appl, string conn_addr) {
  appl->RunServer(conn_addr);
}

// Wrapper to run Daemon from a thread
void StartDaemon(shared_ptr<DedupApplication> appl) { appl->Daemon(); }

int main(int argc, char** argv) {
  int machine_id = atoi(argv[1]);
  int num_threads = atoi(argv[2]);

  // Set Boost log format
  // boost::log::add_console_log(
  //     std::cout, boost::log::keywords::format = "[ %Severity% ] %Message%");

  // Read the controller address and the dedup server address from cluster.json
  boost::property_tree::ptree tree;
  boost::property_tree::read_json("config/cluster.json", tree);

  string addr = tree.get<string>("controller.addr");
  string port = tree.get<string>("controller.port");
  string controller_addr = addr + ":" + port;
  string conn_addr = "";

  BOOST_FOREACH (boost::property_tree::ptree::value_type& v,
                 tree.get_child("grpc_nodes")) {
    int mid = v.second.get<int>("machine_id");
    if (mid == machine_id) {
      string server_addr = v.second.get<string>("addr");
      string server_port = v.second.get<string>("port");
      conn_addr = server_addr + ":" + server_port;
      break;
    }
  }

  // Init CURL global handle
  curl_global_init(CURL_GLOBAL_ALL);

  // Create a DedupApplication and a DedupServerImpl instance and bind each
  // other. Same with MemoryManager and DedupApplication.
  auto service_ptr = std::make_shared<DedupServerImpl>();
  auto manager_ptr = std::make_shared<MemoryManager>(machine_id);
  auto container_map_ptr = std::make_shared<ContainerPointerMap>();

  // Set unlimited message length for gRPC
  grpc::ChannelArguments ch_args;
  ch_args.SetMaxReceiveMessageSize(-1);
  auto channel = CreateCustomChannel(
      controller_addr, grpc::InsecureChannelCredentials(), ch_args);
  auto appl_ptr = std::make_shared<DedupApplication>(
      machine_id, channel, num_threads, container_map_ptr, service_ptr,
      manager_ptr);

  // Read configuration file
  appl_ptr->ReadConfig();

  // NOTE: Need to add shared ptr of appl to service
  service_ptr->set_appl(appl_ptr->get_ptr());
  service_ptr->set_container_map(container_map_ptr);
  manager_ptr->set_appl(appl_ptr->get_ptr());

  // Start the daemon and Dedup Server in parallel threads
  thread server_runner(StartServer, appl_ptr, conn_addr);
  thread daemon_runner(StartDaemon, appl_ptr);

  // Join threads
  server_runner.join();
  daemon_runner.join();

  return 0;
}