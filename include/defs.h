// Header file to include for various definitions

#ifndef DEFS_H
#define DEFS_H

#define PAGE_SIZE 4096

// Enum to hold various states of a container
enum ContainerStatus {
  kRunning = 0,
  kWarm,
  kDedup,
  kBase,
  kDummy,
  kPurge
};

#endif