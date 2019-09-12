# #!/usr/bin/env python
#
# # Note that this file must run with python2.7 to bootstrap conda.
#
# import logging
#
# # TODO(gp): Not sure this is a good idea since it might create cyclic
# # dependencies.
#
# _LOG = logging.getLogger(__name__)
#
# # We cannot use system_interaction since it depends on python3, and this script is
# # used to configure conda to use python3. So to break the cyclic dependency we
# # inline the functions.
# # import helpers.system_interaction as si
#
#
# def get_conda_config_path():
#     path, conda_env_path = _get_conda_config()
#     _ = conda_env_path
#     return path
#
#
# def get_conda_env_path():
#     path, conda_env_path = _get_conda_config()
#     _ = path
#     return conda_env_path
#
#
# def _main():
#     path = get_conda_config_path()
#     print(path)
#
#
# if __name__ == "__main__":
#     _main()
