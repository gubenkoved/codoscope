import argparse
import logging

import coloredlogs
from pandas.core.indexes.base import str_t

from codoscope.config import load_config
from codoscope.reports.base import ReportType
from codoscope.sources.bitbucket import ingest_bitbucket
from codoscope.sources.git import ingest_git_repo, RepoModel
from codoscope.state import load_state, save_sate, StateModel
from codoscope.reports.registry import REPORTS_BY_TYPE

LOGGER = logging.getLogger(__name__)


def ingest(ingestion_config: dict, state: StateModel):
    for source_config in ingestion_config['sources']:
        source_name = source_config['name']
        current_state = state.sources.get(source_name)

        if not source_config.get('enabled', True):
            LOGGER.warning('skip disabled "%s" source', source_name)
            continue

        if source_config['type'] == 'git':
            assert current_state is None or isinstance(current_state, RepoModel)
            source_state = ingest_git_repo(
                current_state,
                source_config['path'],
                source_config['branches'],
                source_config.get('ingestion-limit'),
            )
        elif source_config['type'] == 'bitbucket':
            source_state = ingest_bitbucket(source_config, current_state)
        else:
            raise Exception(f'Unknown source type: {source_config["type"]}')

        state.sources[source_name] = source_state


def entrypoint():
    parser = argparse.ArgumentParser(description='Git stats')
    parser.add_argument('--config-path', type=str, help='Path to config file')
    parser.add_argument('--state-path', type=str, help='Path to the state file')
    parser.add_argument('--log-level', type=str, default='INFO', help='Log level')

    args = parser.parse_args()

    log_level = args.log_level.upper()
    LOGGER.setLevel(log_level)
    coloredlogs.install(level=log_level)

    config = load_config(args.config_path)

    state_path = args.state_path or config.get('state-path')
    if not state_path:
        raise Exception('state-path is not defined in config or passed as argument')

    state = load_state(state_path) or StateModel()

    ingestion_config = config.get('ingestion', {})
    if ingestion_config.get('enabled', True):
        ingestion_rounds = ingestion_config.get('rounds', 1)
        for round_idx in range(1, ingestion_rounds + 1):
            LOGGER.info('start ingestion round %d', round_idx)
            try:
                ingest(ingestion_config, state)
                save_sate(state_path, state)
            except Exception as err:
                LOGGER.error('ingestion round %d failed! %s', round_idx, err)
    else:
        LOGGER.warning('skipped ingestion as requested')

    # render reports
    for report_config in config['reports']:
        report_name = report_config['name']

        if not report_config.get('enabled', True):
            LOGGER.warning('skip disabled "%s" report', report_name)
            continue

        LOGGER.info('render "%s" report', report_name)
        report_class = REPORTS_BY_TYPE.get(ReportType(report_config['type']))

        if report_class is None:
            raise Exception('unable to find report type "%s"', report_config['type'])

        report_instance = report_class()
        report_instance.generate(report_config, state)

    LOGGER.info('completed!')


if __name__ == '__main__':
    entrypoint()
