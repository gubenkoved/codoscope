import argparse
import logging

import coloredlogs

from codoscope.config import load_config
from codoscope.plot import plot_all
from codoscope.sources.bitbucket import ingest_bitbucket
from codoscope.sources.git import ingest_git_repo, RepoModel
from codoscope.state import load_state, save_sate, StateModel

LOGGER = logging.getLogger(__name__)


def entrypoint():
    parser = argparse.ArgumentParser(description='Git stats')
    parser.add_argument('--config-path', type=str, help='Path to config file')
    parser.add_argument('--state-path', type=str, help='Path to the state file')
    parser.add_argument('--out-path', type=str, help='Path to file where HTML will be written')
    parser.add_argument('--log-level', type=str, default='INFO', help='Log level')

    args = parser.parse_args()

    log_level = args.log_level.upper()
    LOGGER.setLevel(log_level)
    coloredlogs.install(level=log_level)

    config = load_config(args.config_path)

    state_path = args.state_path or config.get('state-path')
    if not state_path:
        raise Exception('state-path is not defined in config or passed as argument')

    out_path = args.out_path or config.get('out-path')
    if not out_path:
        raise Exception('out-path is not defined in config or passed as argument')

    state = load_state(state_path) or StateModel()

    for source in config['sources']:
        source_name = source['name']
        current_state = state.sources.get(source_name)

        if source['type'] == 'git':
            assert current_state is None or isinstance(current_state, RepoModel)
            source_state = ingest_git_repo(
                current_state,
                source['path'],
                source['branches'],
                source.get('ingestion-limit'),
            )
        elif source['type'] == 'bitbucket':
            source_state = ingest_bitbucket(source, current_state)
        else:
            raise Exception(f'Unknown source type: {source["type"]}')

        state.sources[source_name] = source_state

    save_sate(state_path, state)

    plot_all(
        state,
        out_path
    )


if __name__ == '__main__':
    entrypoint()
