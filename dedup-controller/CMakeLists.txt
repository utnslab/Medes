# Building Controller
set(SOURCE_FILES
  controller.cc
  controller_server.cc
  controller_server.h
  data_structures.cc
  data_structures.h
  scheduler.cc
)

add_executable(controller ${SOURCE_FILES})

target_link_libraries(controller
  dedup_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  ${Boost_LIBRARIES}
  ${Boost_LOG_LIBRARY}
)