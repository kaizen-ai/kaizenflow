- Documentation is under `documentations/notes`

- Create a tmux session with lemonade + amp set-up
  ```
  cd ~/src/commodity_research1
  ./dev_scripts_p1/tmux_p1.sh part1
  ```
  - TODO(gp): It might be dependent on my set-up and needs to be generalized

- Invariant:
  - We only put in containers what is needed to run, but not development tools
    (otherwise the entire OS should always go)

- `invoke` always runs outside Docker and in the dev shell

- There is a thin env on each machine used for development
  `dev_scripts/client_setup`

- Build the virtual env for dev
  - `dev_scripts/client_setup/build.sh`

- Activate the virtual env
  - `source dev_scripts/client_setup/activate.sh`

- Configure the env
  - `source dev_scripts/setenv_amp.sh`

- Create a docker bash
  ```
  > invoke docker_pull
  > invoke docker_bash
  ```

- Run `invoke run_fast_tests`

- Use AM credentials as default and use `--profile peon`
