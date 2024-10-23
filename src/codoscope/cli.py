import argparse
import logging
import sys

import coloredlogs

from codoscope import core
from codoscope.config import load_config
from codoscope.tools.discover_aliases import discover_aliases

LOGGER = logging.getLogger(__name__)


def entrypoint():
    parser = argparse.ArgumentParser(description="Git stats")
    parser.add_argument("--config-path", type=str, help="Path to config file")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")

    subparsers = parser.add_subparsers(dest="command")
    _ = subparsers.add_parser("process")
    _ = subparsers.add_parser("discover-aliases")

    args = parser.parse_args()

    log_level = args.log_level.upper()
    LOGGER.setLevel(log_level)
    coloredlogs.install(level=log_level)

    config = load_config(args.config_path)

    LOGGER.debug("args: %r", args)

    if args.command == "process":
        core.process(config)
    elif args.command == "discover-aliases":
        discover_aliases(config)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    try:
        entrypoint()
    except Exception as err:
        LOGGER.fatal("unhandled exception: %r", err, exc_info=True)
        sys.exit(1)
