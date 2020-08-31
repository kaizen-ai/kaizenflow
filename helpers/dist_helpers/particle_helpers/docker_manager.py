import argparse

import docker


def get_volumes(name: str):  # pylint: disable=unused-argument
    client = docker.from_env()
    container = client.containers.get("postgres_service")
    return container.attrs["Mounts"]


def get_source_from(name: str):
    s = ""
    for i in get_volumes(name):
        s += i["Source"] + "\n"
    return s


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--get_volumes", required=False, type=str, action="store")
    args = parser.parse_args()
    # _main(args)
    if args.get_volumes:
        print((get_source_from(args.get_volumes)))
