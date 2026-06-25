# Agent Instructions

- Use the project virtual environment at `./venv`. Do not use the system Python for repo tasks.
- Prefer the root `Makefile` for standard workflows instead of ad hoc commands.
- Default commands:
  - `make test` for the test suite
  - `make install` to build and install the package into `venv`
  - `make build` to build distributions
  - `make clean` to remove generated artifacts
- If a task needs a direct Python entrypoint, use `./venv/bin/python ...` from the repo root.
- Run commands from the repository root unless the task requires another working directory.
