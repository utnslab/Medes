cmake_minimum_required(VERSION 3.5.1)

project(serverless-dedup-services C CXX)

# Set DEBUG mode
set(CMAKE_BUILD_TYPE Debug)

# Include the common cmake file to find grpc and protobuf packages
include(cmake/common.cmake)
include(cmake/GenerateCPP.cmake)

# Include Boost libraries
SET(Boost_USE_STATIC_LIBS ON)
FIND_PACKAGE(Boost 1.75 COMPONENTS log REQUIRED)


set(PROTOS
  ${CMAKE_CURRENT_SOURCE_DIR}/protos/main.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/protos/client.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/protos/controller.proto
)

set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto-src)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

# Run gRPC and Protobuyf compilers to generate C++ files
generate_cpp(
  GRPC_SRCS
  GRPC_HDRS
  PROTO_SRCS
  PROTO_HDRS
  ${PROTO_SRC_DIR}
  ${PROTOS})

# Include the genrated *.pb.h, *.grpc.pb.h files
include_directories(${PROTO_SRC_DIR})

# dedup_grpc_proto - A common proto library that can be used by all executables
add_library(dedup_grpc_proto
  ${GRPC_SRCS}
  ${GRPC_HDRS}
  ${PROTO_SRCS}
  ${PROTO_HDRS})
target_link_libraries(dedup_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

set(COMMON_HEADERS_DIR ${PROJECT_SOURCE_DIR}/include)
include_directories(${COMMON_HEADERS_DIR})

# Move the config files into the cmake directory as well
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/config/ DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/config/)

# Move the shell scripts into the cmake directory as well
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/scripts/ DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/scripts/)

add_subdirectory(dedup-controller)
add_subdirectory(dedup-service)
add_subdirectory(rdma)
