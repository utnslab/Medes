
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

/**
 * Get timestamp since epoch
 */
static inline uint64_t gettime(void)
{
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1000000000 + t.tv_nsec;
}

/**
 * Used in threads to set cancel state and type to responding immediately
 * Returns 0 if successful
 */ 
static inline int set_pthread_cancel_immediately(void)
{
  int old_state, old_type;
  if (pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state)) {
    return -1;
  }
  if(pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &old_type)) {
    return -1;
  }
  return 0;
}
