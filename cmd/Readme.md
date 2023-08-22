# CMD

The `cmd` folder holds different applications within raft, in this case a folder for each raft module, and a folder for an example usage of raft. Each contains it's own main package, as well as associated docker-compose file and Dockerfile.

To run one of the applications (from `root` of project):

```bash
docker-compose -f ./cmd/<app-to-run>/docker-compose.yml up --build
```

## Completed

  - test leader election in [leaderelection](./leaderelection)
  - test heartbeat in [heartbeat](./heartbeat)
  - test replicated log in [replog](./replog)

apps to run:
  - leaderelection
  - heartbeat
  - replog