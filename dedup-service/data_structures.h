// Description of the data structures used by the Dedup Application used on each
// host machine

#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include <sys/mman.h>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <chrono>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "defs.h"

// Launch docker checkpoint operation for a container.
// Set the boolean variable true to exit when checkpoint is made.
int DockerCreateCheckpoint(int container_id, bool exit);

// Launch a single docker command given by op, and expected code as given.
int DockerSingleOperation(int container_id, std::string op, int expected_code);

// Launches a specific shell script with name as name, and passes arguments args
void LaunchScript(std::string name, std::string args);

// Computes and returns the hash of a chunk, using the SHA1 algorithm
std::string ComputeChunkHash(char* chunk, int chunk_size);

// Fingerprint calculation functions
std::vector<std::string> FixedOffsetFingerprint(char* page, int num_chunks);
std::vector<std::string> NonNullFingerprint(char* page, int num_chunks);
std::vector<std::string> ValueSampledFingerprint(char* page, int num_chunks);

// Class to represent the information needed to store one base page of a dedup
// container
class DedupInfo {
 public:
  DedupInfo()
      : machine_id_(-1), mr_id_(-1), offset_(-1), patch_length_(PAGE_SIZE) {}

  void SetBasePage(int mid, int mr_id, int offset) {
    machine_id_ = mid;
    mr_id_ = mr_id;
    offset_ = offset;
  }

  void SetPatchLength(int length) { patch_length_ = length; }

  int machine_id() { return machine_id_; }

  int mr_id() { return mr_id_; }

  int offset() { return offset_; }

  int patch_length() { return patch_length_; }

 private:
  // Information about the patch
  int patch_length_;

  // Information about the base page
  int machine_id_;
  int mr_id_;
  int offset_;
};

// Class to store application configuration, shared with all containers of it.
// NOTE: Same configuration shared with all containers of a particular
// application type.
class ApplConfiguration {
 public:
  ApplConfiguration(int keep_alive, int exec_time, double memory)
      : keep_alive_(keep_alive), exec_time_(exec_time), memory_(memory) {}

  int keep_alive_;  // In seconds
  int exec_time_;   // In milliseconds
  double memory_;
};

// Class to store the information about an active Container on the machine
// TODO: Make update calls thread safe
class ContainerInfo {
 public:
  ContainerInfo(int cid, std::string appl)
      : container_id_(cid),
        status_(ContainerStatus::kRunning),
        prev_status_(ContainerStatus::kWarm),
        appl_(appl),
        dedup_map_(),
        checkpointed_(false),
        first_spawned_(true),
        num_dump_files_(1),
        num_pages_(1),
        dump_file_data_(1) {
    last_modified_ = std::chrono::system_clock::now();
    dump_location_ = "/tmp/checkpoints/cont" + std::to_string(cid);

    restore_pid_ = -1;
    cloned_criu_pid_ = -1;
  }

  ContainerStatus UpdateStatus(ContainerStatus status) {
    // Update the prev_status - need to capture amongst BASE/DEDUP/WARM.
    if (status_ != kDummy && status_ != kRunning) {
      prev_status_ = status_;
    }
    if (status != kDummy) {
      // Dummy state is used as an ephemeral state => Does not signify a state
      // change. Hence, not counted to check since when a container has been in
      // a "valid" state.
      last_modified_ = std::chrono::system_clock::now();
    }

    status_ = status;
    // If the updated status if Base or Dedup, then checkpoint has been made
    if (!checkpointed_ && (status == kBase || status == kDedup)) {
      checkpointed_ = true;
    }
  }

  void SetDedupInfo(int page_id, int mid, int mr_id, int offset) {
    dedup_map_.insert(std::make_pair(page_id, std::make_shared<DedupInfo>()));
    dedup_map_.at(page_id)->SetBasePage(mid, mr_id, offset);
  }

  void SetPatch(int page_id, int patch_length) {
    // Check if page_id is already in the dedup_map
    dedup_map_.at(page_id)->SetPatchLength(patch_length);

    // FIXME: Move the find and insert operation in SetDedupInfo
    // if (dedup_map_.find(page_id) != dedup_map_.end()) {
    // } else {
    //   // If not present, add the page_id
    //   dedup_map_.insert(std::make_pair(page_id,
    //   std::make_shared<DedupInfo>()));
    //   dedup_map_.at(page_id)->SetPatchLength(patch_length);
    // }
  }

  bool FindDedupInfo(int page_id) {
    return (dedup_map_.find(page_id) != dedup_map_.end());
  }

  char* GetPageData(int page_id) {
    /*
     * File index 0 -> page ids NONE
     * File index 1 -> page ids 0-len[0]
     * File index 2 -> page ids len[0]-len[1]
     */
    int file_index = 0;
    int offset = page_id;
    for (int num_pages : num_pages_) {
      if (offset < num_pages) {
        break;
      }
      offset -= num_pages;
      file_index++;
    }

    return GetFileData(file_index, offset * PAGE_SIZE);
  }

  char* GetFileData(int file_id, int offset) {
    return &dump_file_data_.at(file_id)[offset];
  }

  void ClearFileData() {
    // M-unmap files in dump_file_data_
    if (dump_file_data_.size() > 1) {
      for (int id = 1; id <= num_dump_files_; id++) {
        munmap(dump_file_data_.at(id), num_pages_.at(id) * PAGE_SIZE);
      }
    }

    dump_file_data_.clear();
  }

  std::unordered_map<int, std::shared_ptr<DedupInfo>>::const_iterator
  DedupMapBegin() {
    return dedup_map_.begin();
  }

  std::unordered_map<int, std::shared_ptr<DedupInfo>>::const_iterator
  DedupMapEnd() {
    return dedup_map_.end();
  }

  std::shared_ptr<DedupInfo> GetDedupInfo(int page_id) {
    return dedup_map_.at(page_id);
  }

  int GetDedupMapSize() { return dedup_map_.size(); }

  void AddDumpFilePages(int num_pages) { num_pages_.push_back(num_pages); }

  int GetNumberOfPages(int file_id) { return num_pages_.at(file_id); }

  void AddFileData(char* file_data) { dump_file_data_.push_back(file_data); }

  void set_restore_pid(int pid) { restore_pid_ = pid; }

  void set_cloned_criu_pid(int pid) { cloned_criu_pid_ = pid; }

  void set_checkpointed() { checkpointed_ = true; }

  void set_first_spawned() { first_spawned_ = false; }

  int container_id() { return container_id_; }

  ContainerStatus status() { return status_; }

  ContainerStatus prev_status() { return prev_status_; }

  std::string dump_location() { return dump_location_; }

  int num_dump_files() { return num_dump_files_; }

  void set_num_dump_files(int num_files) { num_dump_files_ = num_files; }

  bool checkpointed() { return checkpointed_; }

  bool first_spawned() { return first_spawned_; }

  std::chrono::time_point<std::chrono::system_clock> last_modified() {
    return last_modified_;
  }

  std::string appl() { return appl_; }

  int restore_pid() { return restore_pid_; }

  int cloned_criu_pid() { return cloned_criu_pid_; }

 private:
  int container_id_;

  // Status of the container abstracted by the ContainerInfo object
  ContainerStatus status_;

  // Previous status of the container - to which it must revert back
  ContainerStatus prev_status_;

  // Application configuration information
  std::string appl_;

  // Timestamp of last modification of container
  std::chrono::time_point<std::chrono::system_clock> last_modified_;

  // List of Base Pages used, along with mapping from Page ID to the local
  // address (for DEDUP only). Stores only the information for dedup pages
  std::unordered_map<int, std::shared_ptr<DedupInfo>> dedup_map_;

  // Path to file which stores the memory dump of container (if container is
  // BASE or DEDUP)
  // Location : /tmp/checkpoints/cont<container_id>
  std::string dump_location_;

  // Information about the dump files := Needed while recreating the CRIU image
  // files
  int num_dump_files_;
  std::vector<int> num_pages_;

  // Hold num_dump_files_ + 1 entries, to easily index file ids
  std::vector<char*> dump_file_data_;

  // If a container has been checkpointed once, do not launch checkpoint script
  // again.
  bool checkpointed_;

  // Save the restore process pid, to send a signal (for DEDUP only)
  int restore_pid_;
  int cloned_criu_pid_;

  // To emulate checkpoint-restore mechanisms
  bool first_spawned_;
};

class ContainerPointerMap {
 public:
  ContainerStatus GetContainerStatus(int container_id) {
    boost::shared_lock<boost::shared_mutex> lock(container_map_mutex_);
    return local_containers_.at(container_id)->status();
  }

  std::string GetContainerAppl(int container_id) {
    boost::shared_lock<boost::shared_mutex> lock(container_map_mutex_);
    return local_containers_.at(container_id)->appl();
  }

  void AddContainer(int container_id, std::shared_ptr<ContainerInfo> c_info) {
    // Create a shared_ptr to a new ContainerInfo instance and add to
    // local_containers_
    boost::unique_lock<boost::shared_mutex> lock(container_map_mutex_);
    local_containers_.insert(std::make_pair(container_id, c_info));
  }

 private:
  // List of all local containers on machine
  std::unordered_map<int, std::shared_ptr<ContainerInfo>> local_containers_;
  boost::shared_mutex container_map_mutex_;
};

#endif
