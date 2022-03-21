# Building Application
set(SOURCE_FILES
  data_structures.h
  dedup_application.cc
  dedup_application.h
  dedup_server.h
  dedup_server.cc
  memory_manager.h
  memory_manager.cc
  memory_layer.h
  memory_layer.cc
  fingerprint.cc
  includes.cc
)

# add_compile_options(-fno-omit-frame-pointer)

# set_source_files_properties(memory_manager.cc PROPERTIES COMPILE_FLAGS "-I/home/divyanshu/Installers/xdelta-3.1.0/xdelta3 -DSIZEOF_SIZE_T=8 -DSIZEOF_UNSIGNED_LONG_LONG=8")
set_source_files_properties(memory_manager.cc PROPERTIES COMPILE_FLAGS "-DSIZEOF_SIZE_T=8 -DSIZEOF_UNSIGNED_LONG_LONG=8")

set(CURL_LIBRARY "-lcurl") 
find_package(CURL REQUIRED)

add_executable(dedup_appl ${SOURCE_FILES})
target_include_directories(dedup_appl PRIVATE
  ${PROJECT_SOURCE_DIR}/rdma
  ${XDELTA_DIR}
  ${CURL_INCLUDE_DIR}
)
target_link_libraries(dedup_appl
  dedup_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
  ${Boost_LIBRARIES}
  ${Boost_LOG_LIBRARY}
  ${CURL_LIBRARIES}
  rdma
  rdmacm
  ibverbs
)

add_executable(memory_layer_example
  memory_layer_example.cc
  memory_layer.h
  memory_layer.cc
)
target_include_directories(memory_layer_example PRIVATE
  ${PROJECT_SOURCE_DIR}/rdma
)
target_link_libraries(memory_layer_example
  rdma
  rdmacm
  ibverbs
)