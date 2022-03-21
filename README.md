# Medes

Medes is a serverless framework that employs memory deduplication to reduce memory footprints of a warm container.  
It leverages the fact that the warm sandboxes running on serverless platforms have a high fraction of duplication in their memory footprints. It exploits these redundant chunks to develop a new sandbox state, called a dedup state, that is more memory-efficient than the warm state and faster to restore from than the cold state.  
Hence, Medes breaks the rigid trade-off and allows operators to navigate the memory-performance trade-off space smoothly. 

## Implementation Overview

To employ memory deduplication, Medes uses C/R mechanism to fetch the raw memory dump of a warm container.
Then it chooses a representative set of 64B chunks from each page of the dump, and looks up each one of them in a global hash table, that is stored on a global controller.
For each such page of the container to be dedup-ed, this controller employs a simple heuristic to provide addresses of pages (called `base page's) with respect to which, that page shall be deduplicated.
Thereafter, the pages on the dedup-ed container are stored as a patch and a pointer to their respective base pages.
This information is locally stored at the node and at restore, the base pages are read and original pages of the dedup container are restored.

More details of the architecture, system design, optimizations and implementation are available in our paper [here](https://doi.org/10.1145/3492321.3524272).

## Directory Structure

```bash
.
├── CMakeLists.txt
├── cmake
├── config
├── dedup-controller
├── dedup-service
├── evaluation
├── include
├── motivation
├── protos
├── rdma
└── scripts
```

The library for the controller is implemented in `dedup-controller/` while the library for dedup agent on each individual machine is implemented in `dedup-service/`. The dedup agent makes use of RDMA to read local as well as remote pages, a wrapper for which, is available in `rdma/`.  
General include headers are segregated in `include/` directory, while the proto files are located in a common location: `protos/`.  
All scripts needed during the execution of the framework are placed in `scripts/`.  
`motivation/` houses the scripts to run and plot motivation experiments, and all other plotting scripts are placed in `evaluation/`.

## Build and Compile

### Prerequisites

The prerequisites can be automatically installed using the `cloudlab/config.sh` script. Medes requires the following:

- CMake
- gRPC
- Boost (atleast v1.75.0)
- Python and pip
- Docker (v19.03.12)
- xDelta3
- Mellanox OFED (v5.4-3.0.3) and an RNIC
- Openwhisk Python Runtime - modified for Medes. Repo [here](https://github.com/DivyanshuSaxena/openwhisk-runtime-python/tree/dedup)
- CRIU - modified and optimized for Medes. Repo [here](https://github.com/DivyanshuSaxena/criu/tree/mod-criu)

Run `./install.sh` to install all of these dependencies in one go. (**Note:** By default, it will install all dependencies in `$HOME`.)

Finally, to compile, from the top directory, run:

```console
./build.sh
```  

Read `build.sh` to get more information. It assumes that cmake is installed in `$HOME/.local/` and xdelta is installed in `$HOME/xdelta-3.1.0/xdelta3`. If you used the installation script `install.sh`, then these paths are already correct.

## Run

### Configurations

The `config/` folder has two important configuration files: `agent.ini`, used to configure the Dedup agents, and `controller.ini`, used to configure the global controller. Respective files have detailed comments about how to use them.

#### Important Parameters

The above mentioned .ini files expose a few important parameters, as discussed below:

1. `policy`: Chooses which policy is run by the controller, for managing containers.
2. `constraint`: If the Medes policy is chosen, then the platform admin has the flexibility to choose whether they want Medes to function under a memory constraint (get best latency under a fixed memory budget), or a latency constraint (occupy least amount of memory while satisfying a latency bound).
3. `dedupperbase`: The threshold for the ratio of dedup containers to base containers. If the ratio exceeds this threshold, another base container is demarcated.
4. `memcap`: The software-defined limit on the memory usage of each node (in MB).
1. `chunksperpage`: This represents the cardinality of the fingerprint of a page. A higher value implies more chunks shall be sampled from the page, leading to a better duplication identification, but increased communication overheads. _(Default: 5)_
2. `idletime`: This represents the period of time a container remains in the same state (whether warm or dedup), after which the dedup agent shall seek an updated decision for the container from the policy controller.
3. `adaptive`: If set to true(=1), the adaptive keep-alive policy shall be used.

Cluster configuration is required in the `config/cluster.json` file. The format is as follows:

```
{
    "controller": {
        "addr": "<ip-address-of-controller>",
        "port": <port>
    },
    "memory_nodes": [
        {
            "machine_id": 0,
            "addr": "<ip-address-of-machine-with-id-0>",
            "port": <rdma-server-port>
        },
        ...
    ],
    "grpc_nodes": [
        {
            "machine_id": 0,
            "addr": "<ip-address-of-machine-with-id-0>",
            "port": <grpc-port>
        },
        ...
    ]
}
```

To run the controller, from the `cmake/build` directory:

```console
./dedup-controller/controller <number-of-threads> <path-to-requests-file>
```

To run the dedup agent, from the `cmake/build` directory:

```console
./dedup-service/dedup_appl <node-id> <number-of-threads>
```

## Contributions and Contacts

This project is licensed under the [Apache License 2.0](LICENSE).  
For any questions, feel free to contact [Divyanshu Saxena](https://divyanshusaxena.github.io) or [Tao Ji](mailto:taoji@cs.utexas.edu), file issues or submit PRs.