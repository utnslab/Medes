find_package( Threads REQUIRED )

set(SOURCE_FILES
  rcltmgr.c
  rcltmgt.h
  rcomm.c
  rcomm.h
  rparam.h
  rsrvmgr.c
  rsrvmhr.h
  rutil.h
)

add_library(rdma SHARED
${PROJECT_SOURCE_DIR}/rdma/rcomm.c
${PROJECT_SOURCE_DIR}/rdma/rcltmgr.c
${PROJECT_SOURCE_DIR}/rdma/rsrvmgr.c
)

target_link_libraries(rdma
rdmacm
ibverbs
${CMAKE_THREAD_LIBS_INIT}
)