import argparse
import logging

import coloredlogs

from codoscope.config import load_config
from codoscope.sources.git import ingest_git_repo, RepoModel
from codoscope.state import load_state, save_sate, StateModel
from codoscope.plot import plot_all

LOGGER = logging.getLogger(__name__)


def entrypoint():
    parser = argparse.ArgumentParser(description='Git stats')
    parser.add_argument('--config-path', type=str, help='Path to config file')
    parser.add_argument('--state-path', type=str, required=True, help='Path to the state file')
    parser.add_argument('--log-level', type=str, default='INFO', help='Log level')
    parser.add_argument('--out-path', type=str, required=True, help='Path to file where HTML will be written')

    args = parser.parse_args()

    log_level = args.log_level.upper()
    LOGGER.setLevel(log_level)
    coloredlogs.install(level=log_level)

    config = load_config(args.config_path)
    state = load_state(args.state_path) or StateModel()

    for source in config['sources']:
        assert source['type'] == 'git'
        source_name = source['name']
        current_state = state.sources.get(source_name)
        assert current_state is None or isinstance(current_state, RepoModel)
        repo_model = ingest_git_repo(
            current_state,
            source['path'],
            source['branches'],
            source.get('ingestion-limit'),
        )
        state.sources[source_name] = repo_model

    save_sate(args.state_path, state)

    plot_all(
        state,
        args.out_path
    )
