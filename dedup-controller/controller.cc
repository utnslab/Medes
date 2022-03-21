// Entry point to the controller library. CLI Arguments:
// 1: Number of threads that the controller should use in its worker pool
// 2: Request file from which to read the trace

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <iostream>
#include <memory>
#include <thread>

#include "controller_server.h"

using grpc::Server;
using grpc::ServerBuilder;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::thread;

void RunServer(shared_ptr<ControllerServerImpl> service_ptr, string address) {
  std::string server_address(address);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(service_ptr.get());
  // Set maximum limit for gRPC message size
  builder.SetMaxMessageSize(INT_MAX);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  BOOST_LOG_TRIVIAL(info) << "Server listening on " << server_address;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void StartRequestThread(shared_ptr<ControllerServerImpl> service_ptr,
                        string request_file) {
  // Before starting the requests, set up grpc connections with all machines
  service_ptr->ServeRequests(request_file);
}

void SpawnContainersThread(shared_ptr<ControllerServerImpl> service_ptr) {
  service_ptr->SpawnContainers();
}

int main(int argc, char** argv) {
  // Read host address and request file from command line
  int num_threads = atoi(argv[1]);
  string request_file = argv[2];

  // Set Boost log format
  // boost::log::add_console_log(
  //     std::cout, boost::log::keywords::format = "[ %Severity% ] %Message%");

  // Read the grpc connection address from cluster.json file
  boost::property_tree::ptree tree;
  boost::property_tree::read_json("config/cluster.json", tree);

  string addr = tree.get<string>("controller.addr");
  string port = tree.get<string>("controller.port");
  string controller_addr = addr + ":" + port;

  auto server = make_shared<ControllerServerImpl>(num_threads);

  // Read configuration file for the server
  server->ReadConfig();

  // Launch the method for the gRPC Controller Server
  thread server_runner(RunServer, server, controller_addr);
  server->SetupConnections();

  // Launch the method for accepting user requests, after waiting for clients to
  // start
  BOOST_LOG_TRIVIAL(info) << "Waiting for clients to connect";
  std::this_thread::sleep_for(std::chrono::duration<double>(120));
  thread request_runner(StartRequestThread, server, request_file);

  // Not needed := Start the thread to spawn enough containers
  // Instead, increase the purge threshold!
  // thread spawn_containers(SpawnContainersThread, server);

  // Join threads
  server_runner.join();
  request_runner.join();
  // spawn_containers.join();

  return 0;
}