## Scripts

The directory holds all the scripts that are used for the installation and running of experiments.

### Directory Structure

```bash
.
├── README.md
├── appl
├── deps
├── docker
├── install
├── lxc
├── cleanup.sh
├── remove_nonpages.sh
├── remove_pages.sh
├── spawn.sh
├── start_agent.sh
├── start_controller.sh
└── test_docker.sh
```

The directories `deps`, `docker` and `lxc` are deprecated. `deps` and `lxc` are no longer used because all
container runtime operations are switched to the Openwhisk Python Runtime. Further, all docker commands are issued
directly to the Docker daemon and hence the `docker` scripts directory is unused.
