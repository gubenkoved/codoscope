import collections
import logging

import pandas
import yaml

from codoscope import core
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.exceptions import InvalidOperationError
from codoscope.state import StateModel

LOGGER = logging.getLogger(__name__)


def discover_aliases(config: dict):
    """
    Discovers aliases and returns map from canonical name to collection of
    AliasDescriptor instances.
    """
    state_path = read_mandatory(config, "state-path")
    state = StateModel.load(state_path)

    if state is None:
        raise InvalidOperationError("state not found at %s", state_path)

    # extract data sets from the state
    datasets = Datasets.extract(state)
    LOGGER.info("datasets extraction completed")

    # run existing processors to account for existing user remap config
    core.run_processors(config, datasets)

    email_to_name = collections.defaultdict(set)
    name_to_email = collections.defaultdict(set)
    name_counter = collections.defaultdict(int)
    email_counter = collections.defaultdict(int)

    for row in datasets.activity.itertuples():
        name = row.author
        email = row.author_email
        if not pandas.isna(name):
            name_counter[name] += 1
        if not pandas.isna(email):
            email_counter[email] += 1
        if not pandas.isna(email) and not pandas.isna(name):
            email_to_name[email].add(name)
            name_to_email[name].add(email)

    # TODO: consider rewriting in terms of graph nodes (of type name and email)
    # TODO: make process case insensitive
    # TODO: find a good way to preserve existing aliases
    #  consider the case where "Foo" was remapped to "Bar" -- we probably need to
    #  feed "Foo" - "Bar" association into the graph before processing
    traversed_names = set()
    traversed_emails = set()

    components = []

    NodeDef = tuple[str | None, str | None]

    def traverse(node: NodeDef) -> list[NodeDef]:
        result = [node]
        if node[0] is not None:
            # traverse connected emails
            for email in name_to_email[node[0]]:
                if email not in traversed_emails:
                    traversed_emails.add(email)
                    result.extend(traverse((None, email)))
        elif node[1] is not None:
            # traverse connected names
            for name in email_to_name[node[1]]:
                if name not in traversed_names:
                    traversed_names.add(name)
                    result.extend(traverse((name, None)))
        return result

    for email in email_counter:
        if email not in traversed_emails:
            component = traverse((None, email))
            components.append(component)

    for name in name_counter:
        if name not in traversed_names:
            component = traverse((name, None))
            components.append(component)

    # process connected components and compose canonical names
    canonical_names = {}
    for component in components:
        # pick the most common name
        canonical_name = max(component, key=lambda x: name_counter.get(x[0], 0))[0]
        if not canonical_name:
            LOGGER.warning("skipped component with no canonical name")
        aliases = []
        alias_emails = set()
        alias_names = set()
        for name, email in component:
            if email:
                alias_emails.add(email)
            if name:
                alias_names.add(name)
        for alias_email in sorted(alias_emails):
            aliases.append(
                {
                    "email": alias_email,
                }
            )
        for alias_name in sorted(alias_names):
            if alias_name == canonical_name:
                continue
            aliases.append(
                {
                    "name": alias_name,
                }
            )
        canonical_names[canonical_name] = aliases

    canonical_names = dict(sorted(canonical_names.items()))
    LOGGER.info("printing results to stdout")
    print(yaml.dump(canonical_names, allow_unicode=True))
    LOGGER.info("total amount of canonical names: %d", len(canonical_names))
