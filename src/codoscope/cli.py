import argparse
import logging

import coloredlogs

from codoscope.config import load_config, read_mandatory
from codoscope.datasets import Datasets
from codoscope.exceptions import ConfigError
from codoscope.processors.remap_users import RemapUsersProcessor
from codoscope.reports.common import ReportType
from codoscope.reports.registry import REPORTS_BY_TYPE
from codoscope.sources.bitbucket import ingest_bitbucket
from codoscope.sources.git import ingest_git_repo, RepoModel
from codoscope.sources.jira import ingest_jira
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


def ingest(ingestion_config: dict, state: StateModel):
    for source_config in ingestion_config['sources']:
        source_name = source_config['name']
        current_state = state.sources.get(source_name)

        LOGGER.info('ingesting "%s" source', source_name)

        if not source_config.get('enabled', True):
            LOGGER.warning('skip disabled "%s" source', source_name)
            continue

        if source_config['type'] == 'git':
            assert current_state is None or isinstance(current_state, RepoModel)
            source_state = ingest_git_repo(
                source_config,
                current_state,
                source_config['path'],
                source_config['branches'],
                source_config.get('ingestion-limit'),
            )
        elif source_config['type'] == 'bitbucket':
            source_state = ingest_bitbucket(
                source_config,
                current_state,
            )
        elif source_config['type'] == 'jira':
            source_state = ingest_jira(
                source_config,
                current_state,
            )
        else:
            raise ConfigError(f'Unknown source type: {source_config["type"]}')

        state.sources[source_name] = source_state


def entrypoint():
    parser = argparse.ArgumentParser(description='Git stats')
    parser.add_argument('--config-path', type=str, help='Path to config file')
    parser.add_argument('--log-level', type=str, default='INFO', help='Log level')

    args = parser.parse_args()

    log_level = args.log_level.upper()
    LOGGER.setLevel(log_level)
    coloredlogs.install(level=log_level)

    config = load_config(args.config_path)

    state_path = read_mandatory(config, 'state-path')
    state = StateModel.load(state_path) or StateModel()

    ingestion_config = config.get('ingestion', {})
    if ingestion_config.get('enabled', True):
        ingestion_rounds = ingestion_config.get('rounds', 1)
        for round_idx in range(1, ingestion_rounds + 1):
            LOGGER.info('ingestion round #%d of %d', round_idx, ingestion_rounds)
            try:
                ingest(ingestion_config, state)
                state.save(state_path)
            except Exception as err:
                LOGGER.error('ingestion round #%d failed! %r', round_idx, err)
    else:
        LOGGER.warning('skipped ingestion as requested')

    # extract data sets from the state
    datasets = Datasets(state)
    datasets.extract()
    LOGGER.info('datasets extraction completed')

    # TODO: make it more generic instead of dependency of specific named dataset
    for processor_config in config.get('processors', []):
        processor_name = processor_config['name']
        processor_type = processor_config['type']
        LOGGER.info('handling "%s" processor', processor_name)
        if processor_type == 'remap-users':
            processor = RemapUsersProcessor(processor_config)
            processor.execute(datasets.activity)
        else:
            raise ConfigError('unknown processor type: "%s"' % processor_type)

    # render reports
    for report_config in config.get('reports', []):
        report_name = report_config['name']

        if not report_config.get('enabled', True):
            LOGGER.warning('skip disabled "%s" report', report_name)
            continue

        LOGGER.info('render "%s" report', report_name)
        report_class = REPORTS_BY_TYPE.get(ReportType(report_config['type']))

        if report_class is None:
            raise ConfigError('unable to find report type "%s"', report_config['type'])

        report_instance = report_class()
        report_instance.generate(report_config, state, datasets)

    LOGGER.info('completed!')


if __name__ == '__main__':
    entrypoint()
