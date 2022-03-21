// Impl of the Memory Manager class

#include "memory_manager.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

#include <boost/asio/post.hpp>
#include <boost/log/trivial.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "controller.pb.h"
#include "data_structures.h"
#include "xdelta3.c"
#include "xdelta3.h"

using boost::asio::post;
using dedup::BaseContainerResponse;
using std::ifstream;
using std::ofstream;
using std::shared_ptr;
using std::string;
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::system_clock;

using BasePageStruct = dedup::BaseContainerResponse_BasePage;

void MemoryManager::_CompletePendingRequests() {
  // Call MemoryLayer::post_completion for all pending requests
  int ret;
  MemoryCompletion mcomp;

  for (const auto& pair : remaining_req_) {
    int machine_id = pair.first;
    for (int i = 0; i < pair.second; i++) {
      while ((ret = ml_->poll_completion(machine_id, mcomp)) == 1)
        ;
      if (ret == -1 || mcomp.status) {
        BOOST_LOG_TRIVIAL(error) << "Failed to poll from remote machine";
      }
    }
  }

  // Clear pending requests and reset
  remaining_req_.clear();
  pending_reqs_ = 0;
}

int MemoryManager::AddMemoryRequest(MemoryRequest req) {
  std::lock_guard<std::mutex> guard(remaining_req_mutex_);
  int res = ml_->post_request(req);

  int remote_machine = req.machine_id;
  remaining_req_[remote_machine] += 1;
  pending_reqs_++;

  // NOTE: Can only send a limited number of requests at a time. Complete
  // pending requests before moving ahead.
  if (pending_reqs_ >= MAX_NUM_PENDING_REQS) {
    // Call completion of pending requests to accommodate more
    _CompletePendingRequests();
  }

  return res;
}

void MemoryManager::CompletePendingRequests() {
  std::lock_guard<std::mutex> guard(remaining_req_mutex_);
  _CompletePendingRequests();
}

void MemoryManager::EncodePage(const uint8_t* page_buf,
                               const uint8_t* base_page, char* output,
                               int* output_size, int* count,
                               std::mutex& count_mutex) {
  int compression_level = 1;
  int flags = (compression_level << XD3_COMPLEVEL_SHIFT) & XD3_COMPLEVEL_MASK;

  long unsigned int delta_alloc = 1.5 * PAGE_SIZE;
  char* delta_buf = new char[delta_alloc];
  long unsigned int delta_size;

  // Call the Xdelta3 encode function
  int ret = xd3_encode_memory(page_buf, PAGE_SIZE, base_page, PAGE_SIZE,
                              (unsigned char*)delta_buf, &delta_size,
                              delta_alloc, flags);
  if (ret == 0) {
    if (delta_size < patch_threshold_) {
      *output_size = delta_size;
      memcpy(output, delta_buf, delta_size);
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "Could not encode memory " << delta_size;
  }

  delete[] delta_buf;

  std::lock_guard<std::mutex> guard(count_mutex);
  *count += 1;
}

void MemoryManager::DecodePage(const uint8_t* delta, int delta_size,
                               const uint8_t* base_page, char* output,
                               int* count, std::mutex& count_mutex) {
  int compression_level = 1;
  int flags = (compression_level << XD3_COMPLEVEL_SHIFT) & XD3_COMPLEVEL_MASK;

  long unsigned int page_alloc = 1.5 * PAGE_SIZE;
  char* page_buf = new char[page_alloc];
  long unsigned int computed_size;

  // Call the Xdelta3 decode function
  int ret = xd3_decode_memory(delta, delta_size, base_page, PAGE_SIZE,
                              (unsigned char*)page_buf, &computed_size,
                              page_alloc, flags);
  if (ret == 0) {
    if (computed_size == PAGE_SIZE) {
      memcpy(output, page_buf, PAGE_SIZE);
    } else {
      BOOST_LOG_TRIVIAL(error) << "Incorrect decode " << computed_size;
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "Could not decode memory";
  }

  delete[] page_buf;

  std::lock_guard<std::mutex> guard(count_mutex);
  *count += 1;
}

/* The `resp' argument contains the list of base pages sent by the Controller
 * (at most, one base page per container page). Iterate over the list, and
 * register a single destination address to read the remote page */
void MemoryManager::DeduplicateDump(shared_ptr<ContainerInfo> container,
                                    shared_ptr<BaseContainerResponse> resp) {
  // BOOST_LOG_TRIVIAL(trace) << "Started Deduplicate dump procedure";
  auto start_time = system_clock::now();

  /*
   * 1. Read the response from the server and set the dedup information of each
   * dedup container page.
   * 2. Simultaneously send remote memory reads and wait for their completion
   * 3. Call the Xdelta library to get patches for each of these pages.
   * 4. Update the patch information in the container dedup pages, and write the
   * compressed data into a new file.
   */

  // STEP 1 AND 2 - REMOTE READS
  // Iterate over all base page structs and read remote pages accordingly
  int num_dedup_pages = resp->basepagelist_size();
  BOOST_LOG_TRIVIAL(trace) << "Num dedup pages: " << num_dedup_pages;

  // Register a single large destination mr, and use different offsets for
  // various remote base pages.
  void* dst_buf = mmap(0, PAGE_SIZE * num_dedup_pages, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  MemoryRegionID dst_mrid =
      ml_->reg_dst_mr((void*)dst_buf, PAGE_SIZE * num_dedup_pages);
  if (dst_mrid < 0) {
    BOOST_LOG_TRIVIAL(error)
        << "Failed to register dst mr " << dst_mrid << " " << strerror(errno);
  }

  // Use the local memory address to calculate destination buffer address
  ibv_mr* lmr = ml_->lookup_raw_local_mr(dst_mrid);
  if (!lmr) {
    BOOST_LOG_TRIVIAL(error) << "Failed to find local mr";
  }

  // The read pages go into this map of page_id-offsets
  std::unordered_map<int, int> dst_offsets;

  int local_base_pages = 0;
  int remote_base_pages = 0;

  int num_failed = 0;

  for (int it = 0; it < num_dedup_pages; it++) {
    BasePageStruct base_struct = resp->basepagelist(it);
    int remote_machine = base_struct.machineid();
    int remote_mrid = base_struct.mrid();
    int remote_offset = base_struct.baseaddr();
    int page_id =
        base_struct.addr(); /* Page ID sent in the GetBaseContainers() rpc */

    // Set the Dedup Info in the container object
    container->SetDedupInfo(page_id, remote_machine, remote_mrid,
                            remote_offset);

    // Save the page id along with the offset on the destination MR
    dst_offsets.insert(std::make_pair(page_id, it));

    // Use the memory layer to fetch the remote page
    ibv_mr* rmr = ml_->lookup_raw_remote_mr(remote_machine, remote_mrid);
    if (!rmr) {
      BOOST_LOG_TRIVIAL(error) << "Failed to find remote mr";
    }

    // Create the Memory Request object
    MemoryRequest req = {.context = (void*)page_id,
                         .machine_id = remote_machine,
                         .remote_addr = rmr->addr + remote_offset,
                         .length = PAGE_SIZE,
                         .remote_mr = remote_mrid,
                         .local_addr = lmr->addr + it * PAGE_SIZE,
                         .local_mr = dst_mrid};

    if (remote_machine == machine_id_) {
      local_base_pages += 1;
    } else {
      remote_base_pages += 1;
    }

    if (AddMemoryRequest(req)) {
      num_failed++;
      BOOST_LOG_TRIVIAL(error)
          << "Failed to post read to " << remote_machine << " at page " << it;
      continue;
    }
  }

  // Read all pending requests - acts as barrier to complete all reads before
  // proceeding
  CompletePendingRequests();

  // Clear the gRPC object
  resp->clear_basepagelist();

  if (num_failed > 0) {
    BOOST_LOG_TRIVIAL(error) << "Failed remote reads: " << num_failed;
  }

  // STEP 3 - ENCODINGS
  auto encode_start_time = system_clock::now();

  // Register a single large enough buffer to store all patches
  char* patch_buf = static_cast<char*>(
      mmap(0, patch_threshold_ * num_dedup_pages, PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

  // Store all delta sizes in an unordered map : page_id -> delta_size
  std::unordered_map<int, int> delta_sizes;
  int completed = 0;
  std::mutex dedup_mutex;

  for (auto& offset_pair : dst_offsets) {
    int page_id = offset_pair.first;
    int offset = offset_pair.second;

    // Insert the page_id in the delta map
    delta_sizes.insert(std::make_pair(page_id, -1));

    char* page_buf = container->GetPageData(page_id);
    post(workers_,
         std::bind(&MemoryManager::EncodePage, this,
                   (const unsigned char*)page_buf,
                   (const unsigned char*)(lmr->addr + offset * PAGE_SIZE),
                   &patch_buf[offset * patch_threshold_], &delta_sizes[page_id],
                   &completed, std::ref(dedup_mutex)));
  }

  BOOST_LOG_TRIVIAL(trace) << "Waiting for requests to be completed";

  // Wait for completion of the above requests
  while (completed != num_dedup_pages)
    ;

  // Cleanup - deregister and delete the registered buffer
  ml_->dereg_mr(dst_mrid);

  // Unmap the destination buffer and patch buffer
  munmap(dst_buf, PAGE_SIZE * num_dedup_pages);

  auto encode_end_time = system_clock::now();

  // STEP 4 - WRITE INTO PATCH FILE
  // Collect stats
  long unsigned int success_dedup_pages = 0;
  long unsigned int benefit = 0;
  double average_delta_size = 0.0;

  // Write the deduplicated pages to this patch file
  ofstream patch_file(container->dump_location() + "/patch-file.img",
                      std::ios::out | std::ios::binary);
  if (!patch_file) {
    BOOST_LOG_TRIVIAL(error) << "Could not open patch file for writing";
  }

  // Iterate through all the file data of dump files, and write into patch file
  int page_id = 0;
  for (int file_index = 1; file_index <= container->num_dump_files();
       file_index++) {
    int file_pages = container->GetNumberOfPages(file_index);
    for (int it = 0; it < file_pages; it++) {
      bool written = false;
      char* page_buf = container->GetFileData(file_index, it * PAGE_SIZE);

      // Write patch if dedup was successful
      if (delta_sizes.find(page_id) != delta_sizes.end()) {
        long unsigned int delta_size = delta_sizes[page_id];
        if (delta_size > 0 && delta_size <= patch_threshold_) {
          // Encoding successful
          // Computed patch is below the threshold - write into file and erase
          // from the object
          container->SetPatch(page_id, delta_size);
          patch_file.write(&patch_buf[dst_offsets[page_id] * patch_threshold_],
                           delta_size);

          // Update stats
          success_dedup_pages += 1;
          benefit += (PAGE_SIZE - delta_size);
          average_delta_size += delta_size;

          written = true;
        }
      }

      if (!written) {
        // Write the original page as it is
        patch_file.write(page_buf, PAGE_SIZE);
      }

      page_id += 1;
    }
  }

  // Finish up dedup operation now
  patch_file.close();

  // Unmap the patch buffer
  munmap(patch_buf, patch_threshold_ * num_dedup_pages);
  auto end_time = system_clock::now();

  // Log statistics
  int cid = container->container_id();
  string env = container->appl();
  BOOST_LOG_TRIVIAL(info) << "Successful dedup pages " << env << " "
                          << success_dedup_pages << "/" << num_dedup_pages;
  double benefit_in_mb = ((double)benefit) / (1024 * 1024);
  BOOST_LOG_TRIVIAL(info) << "Dedup benefit " << env << " " << benefit_in_mb;
  BOOST_LOG_TRIVIAL(info) << "Average Delta Size " << env << " "
                          << (average_delta_size / success_dedup_pages);
  BOOST_LOG_TRIVIAL(info) << "Container overhead " << env << " "
                          << num_dedup_pages * sizeof(DedupInfo);
  BOOST_LOG_TRIVIAL(info) << "Local Base pages: " << local_base_pages << "/"
                          << num_dedup_pages;
  BOOST_LOG_TRIVIAL(info) << "Remote Base pages: " << remote_base_pages << "/"
                          << num_dedup_pages;

  auto exec_time =
      duration_cast<duration<double, std::milli>>(end_time - start_time)
          .count();
  auto encode_time = duration_cast<duration<double, std::milli>>(
                         encode_end_time - encode_start_time)
                         .count();
  BOOST_LOG_TRIVIAL(info) << "ML dedup time " << cid << " " << exec_time;
  BOOST_LOG_TRIVIAL(info) << "ML Encode time " << cid << " " << encode_time;

  if (auto appl_ptr = appl_.lock()) {
    if (no_pause_) {
      appl_ptr->LaunchContainerDedupNoPause(container->container_id(), false);
    } else {
      appl_ptr->LaunchContainerDedup(container->container_id(), false);
    }
  }
}

void MemoryManager::RestoreDump(shared_ptr<ContainerInfo> container) {
  // BOOST_LOG_TRIVIAL(trace) << "Started restore dump procedure";
  auto start_time = system_clock::now();

  /*
   * 1. Read the remote locations from the relevant DedupInfos in the dedup_map_
   * data structure.
   * 2. Read the patch file, one page at a time and use Xdelta library to
   * recompute the original pages
   * 3. Write the original pages in the CRIU dump file.
   */

  int num_dedup_pages = container->GetDedupMapSize();
  BOOST_LOG_TRIVIAL(trace) << "Num dedup pages: " << num_dedup_pages;

  // STEP 1 - REMOTE READS
  // Register a single large destination mr, and use different offsets for
  // various remote base pages.
  void* dst_buf = mmap(0, PAGE_SIZE * num_dedup_pages, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  MemoryRegionID dst_mrid =
      ml_->reg_dst_mr(dst_buf, PAGE_SIZE * num_dedup_pages);
  if (dst_mrid < 0) {
    BOOST_LOG_TRIVIAL(error)
        << "Failed to register dst mr " << dst_mrid << " " << strerror(errno);
  }

  // Use the local memory address to calculate destination buffer address
  ibv_mr* lmr = ml_->lookup_raw_local_mr(dst_mrid);
  if (!lmr) {
    BOOST_LOG_TRIVIAL(error) << "Failed to find local mr";
  }

  // The read pages go into this map of page_id-offsets
  std::unordered_map<int, int> dst_offsets;

  int num_failed = 0;
  int offset = 0;
  for (auto dedup_pair = container->DedupMapBegin();
       dedup_pair != container->DedupMapEnd(); ++dedup_pair) {
    int page_id = dedup_pair->first;
    auto dedup_info = dedup_pair->second;

    // Check if a patch was created for this page_id (by checking machine_id)
    if (dedup_info->machine_id() == -1) continue;

    int remote_machine = dedup_info->machine_id();
    int remote_mrid = dedup_info->mr_id();
    int remote_offset = dedup_info->offset();

    // Use the memory layer to fetch the remote page
    ibv_mr* rmr = ml_->lookup_raw_remote_mr(remote_machine, remote_mrid);
    if (!rmr) {
      BOOST_LOG_TRIVIAL(error) << "Failed to find remote mr";
    }

    // Save the page id along with the offset on the destination MR
    dst_offsets.insert(std::make_pair(page_id, offset * PAGE_SIZE));
    offset++;

    // Create the Memory Request object
    MemoryRequest req = {.context = (void*)page_id,
                         .machine_id = remote_machine,
                         .remote_addr = rmr->addr + remote_offset,
                         .length = PAGE_SIZE,
                         .remote_mr = remote_mrid,
                         .local_addr = lmr->addr + dst_offsets[page_id],
                         .local_mr = dst_mrid};

    if (AddMemoryRequest(req)) {
      num_failed++;
      BOOST_LOG_TRIVIAL(error) << "Failed to post read to " << remote_machine
                               << " at page " << page_id;
      continue;
    }
  }

  // Read all pending requests
  CompletePendingRequests();

  if (num_failed > 0) {
    BOOST_LOG_TRIVIAL(error) << "Failed remote reads: " << num_failed;
  }

  // STEP 2 - DECODINGS
  auto decode_start_time = system_clock::now();

  // Register a single large enough region
  int total_pages = 0;
  for (int file_index = 1; file_index <= container->num_dump_files();
       file_index++) {
    int file_pages = container->GetNumberOfPages(file_index);
    total_pages += file_pages;
  }
  char* original_buf = static_cast<char*>(
      mmap(0, PAGE_SIZE * total_pages, PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

  int completed = 0;
  std::mutex restore_mutex;
  std::vector<char*> patch_buffers;

  // Read the patch file and construct the CRIU image dump files
  ifstream patch_file(container->dump_location() + "/patch-file.img",
                      std::ios::out | std::ios::binary);

  // Start reading from patch file and launch decode op on threadpool
  int page_id = 0;
  for (int file_index = 1; file_index <= container->num_dump_files();
       file_index++) {
    int file_pages = container->GetNumberOfPages(file_index);
    for (int it = 0; it < file_pages; it++) {
      // Get the length of the patch to be read
      int length = PAGE_SIZE;
      if (container->FindDedupInfo(page_id)) {
        length = container->GetDedupInfo(page_id)->patch_length();
      }
      char* read_patch = new char[length];
      patch_file.read(read_patch, length);

      if (length == PAGE_SIZE) {
        // Write the read contents as it is.
        memcpy(&original_buf[page_id * PAGE_SIZE], read_patch, length);
        delete[] read_patch;
      } else {
        // Compute original page by decoding memory
        post(workers_,
             std::bind(&MemoryManager::DecodePage, this,
                       (const unsigned char*)read_patch, length,
                       (const unsigned char*)(lmr->addr + dst_offsets[page_id]),
                       &original_buf[page_id * PAGE_SIZE], &completed,
                       std::ref(restore_mutex)));
        patch_buffers.push_back(read_patch);
      }

      page_id += 1;
    }
  }

  BOOST_LOG_TRIVIAL(trace) << "Waiting for requests to be completed";

  // Wait for completion of the above requests
  while (completed != num_dedup_pages)
    ;

  // Close file and buffers
  patch_file.close();
  for (int i = 0; i < patch_buffers.size(); i++) {
    delete[] patch_buffers.at(i);
  }

  // Cleanup - deregister and delete the registered buffer
  ml_->dereg_mr(dst_mrid);

  // Unmap the destination buffer
  munmap(dst_buf, PAGE_SIZE * num_dedup_pages);

  // STEP 3 - WRITE INTO DUMPS
  page_id = 0;
  for (int file_index = 1; file_index <= container->num_dump_files();
       file_index++) {
    int file_pages = container->GetNumberOfPages(file_index);
    ofstream dump_file(container->dump_location() + "/pages-" +
                           std::to_string(file_index) + ".img",
                       std::ios::out | std::ios::binary);

    dump_file.write(&original_buf[page_id * PAGE_SIZE], file_pages * PAGE_SIZE);
    dump_file.close();

    page_id += file_pages;
  }

  // Unmap the original buffer
  munmap(original_buf, PAGE_SIZE * total_pages);

  auto end_time = system_clock::now();

  int cid = container->container_id();
  auto decode_time =
      duration_cast<duration<double, std::milli>>(end_time - decode_start_time)
          .count();
  BOOST_LOG_TRIVIAL(info) << "ML Decode time " << cid << " " << decode_time;
}