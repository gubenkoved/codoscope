import collections
import logging

import pandas
import yaml

from codoscope import core
from codoscope.config import read_mandatory
from codoscope.datasets import Datasets
from codoscope.exceptions import InvalidOperationError
from codoscope.processors.common import ProcessorType
from codoscope.processors.remap_users import RemapUsersProcessor
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

    class Node:
        def __init__(self, node_type: str, value: str):
            self.node_type: str = node_type
            self.value: str = value
            self.use_counter: int = 0

        def inc_use_counter(self):
            self.use_counter += 1

    adjacency_list = collections.defaultdict(set)
    nodes_index: dict[tuple[str, str], Node] = {}

    def ensure_node(node_type: str, value: str):
        key = (node_type, value)
        if key not in nodes_index:
            nodes_index[key] = Node(node_type, value)
        return nodes_index[key]

    def add_association(node1: Node, node2: Node):
        adjacency_list[node1].add(node2)
        adjacency_list[node2].add(node1)

    def process_associations(activity_df: pandas.DataFrame, maintain_use_counter: bool):
        LOGGER.info("capturing associations between names and emails...")
        for item in activity_df.itertuples():
            name_node, email_node = None, None
            if not pandas.isna(item.user):
                name_node = ensure_node("name", item.user)
                if maintain_use_counter:
                    name_node.inc_use_counter()
            if not pandas.isna(item.user_email):
                email_node = ensure_node("email", item.user_email)
                if maintain_use_counter:
                    email_node.inc_use_counter()

            # record association when both are available
            if name_node and email_node:
                add_association(name_node, email_node)

    def capture_associations_from_existing_remap_users_processors():
        for processor_config in config.get("processors", []):
            processor_type = read_mandatory(processor_config, "type")
            if processor_type == ProcessorType.REMAP_USERS.value:
                remapper = RemapUsersProcessor(processor_config)

                for name, canonical_name in remapper.name_to_canonical_name_map.items():
                    name_node = ensure_node("name", name)
                    canonical_name_node = ensure_node("name", canonical_name)
                    add_association(name_node, canonical_name_node)

    # capture association from existing config to preserve aliases for edge
    # cases like name only user being remapped to completely different name
    # so that such information is not inferrable anymore from the result of
    # remapping
    capture_associations_from_existing_remap_users_processors()

    # run existing processors to account for existing user remap config
    core.run_processors(config, datasets)

    # process capture association after possible remapping happened
    process_associations(datasets.get_all_activity(), maintain_use_counter=True)

    traversed: set[Node] = set()
    components: list[set[Node]] = []

    def traverse(node: Node) -> set[Node]:
        result = set([node])
        for associated_node in adjacency_list.get(node, []):
            if associated_node not in traversed:
                traversed.add(associated_node)
                # traverse recursively to other nodes
                result.update(traverse(associated_node))
        return result

    LOGGER.info("detected connected comonets in association graph...")
    for node in nodes_index.values():
        if node in traversed:
            continue
        components.append(traverse(node))
    LOGGER.info("detected connected comonets in association graph... completed")

    # process connected components and compose canonical names
    canonical_names = {}
    for component in components:
        # pick the most common name (or use email if no names found)
        canonical_name_node = max(component, key=lambda x: (x.node_type == "name", x.use_counter))
        assert canonical_name_node is not None
        canonical_name = canonical_name_node.value

        if canonical_name_node.node_type == "email":
            LOGGER.warning('using email "%s" as canonical name', canonical_name)

        aliases = []
        alias_emails = set()
        alias_names = set()

        for node in component:
            if node.node_type == "email":
                alias_emails.add(node.value)
            elif node.node_type == "name":
                alias_names.add(node.value)

        for alias_email in sorted(alias_emails):
            aliases.append({"email": alias_email})

        for alias_name in sorted(alias_names):
            if alias_name == canonical_name:
                continue
            aliases.append({"name": alias_name})
        canonical_names[canonical_name] = aliases

    canonical_names = dict(sorted(canonical_names.items()))
    LOGGER.info("printing results to stdout")
    print(yaml.dump(canonical_names, allow_unicode=True))
    LOGGER.info("total amount of canonical names: %d", len(canonical_names))
