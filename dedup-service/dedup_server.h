// Server for the DedupService gRPC server

#ifndef DEDUP_SERVER_H
#define DEDUP_SERVER_H

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <vector>

#include "client.grpc.pb.h"
#include "dedup_application.h"

using dedup::Ack;
using dedup::Container;
using dedup::DedupService;
using dedup::MemoryResponse;
using dedup::SpawnArgs;
using grpc::ServerContext;
using grpc::Status;

// Forward Declaration of DedupApplication
class DedupApplication;

class DedupServerImpl final : public DedupService::Service {
 public:
  explicit DedupServerImpl() : cold_start_(true), appl_() {}

  void set_appl(std::shared_ptr<DedupApplication> appl) { appl_ = appl; }

  void set_container_map(std::shared_ptr<ContainerPointerMap> container_map) {
    container_map_ = container_map;
  }

  Status Spawn(ServerContext* ctxt, const SpawnArgs* args,
               MemoryResponse* resp);

  Status Restart(ServerContext* ctxt, const Container* args,
                 MemoryResponse* resp);

  Status Restore(ServerContext* ctxt, const Container* args,
                 MemoryResponse* resp);

  Status Purge(ServerContext* ctxt, const Container* args,
               MemoryResponse* resp);

  Status Terminate(ServerContext* ctxt, const Ack* args, Ack* resp);

 private:
  bool cold_start_;
  std::shared_ptr<ContainerPointerMap> container_map_;
  std::weak_ptr<DedupApplication> appl_;
};

#endif