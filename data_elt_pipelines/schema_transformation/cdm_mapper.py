# given the input as a csv stream (StringIO), and a mapping file as dictionary,
# this function will return a csv string (StringIO) based on the mapping file

import re
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Tuple, Generator
from schema_transformation.cdm_mapping_rule import (
    MappingRule,
    MappingRules,
    Strategy,
    ValueType,
)
from shared.cdm_company import CdmCompany, OrderedEnum
from shared.metadata import CdmFileMetaData
from shared.constants import DEFAULT_MAX_DATA_CHUNK_SIZE

logger = logging.getLogger()


def is_metadata_set(
    cdm_model: Dict[str, Any],
    metadata: CdmFileMetaData | None = None,
) -> bool:
    """
    check if the metadata is set in the cdm model
    """

    if not metadata:
        raise ValueError("metadata is required")
    metadata_fields = metadata.model_dump().keys()

    if not cdm_model:
        return False
    for field in metadata_fields:
        if field not in cdm_model:
            return False
    return True


def set_metadata(
    cdm_model: dict[str, Any], metadata: CdmFileMetaData
) -> Dict[str, Any]:
    """
    set the metadata in the cdm model
    """
    if not metadata:
        return cdm_model
    file_metadata = metadata.model_dump()
    for key, value in file_metadata.items():
        if key not in cdm_model:
            cdm_model[key] = value
        elif cdm_model[key] is None:
            cdm_model[key] = value
    return cdm_model


def validate_mapping_rules(mapping_rules: Dict[str, any]) -> Dict[str, MappingRule]:
    """
    Get the mapping rules based on the content type, jurisdiction and optional version,
    if no version is provided, use the latest version
    """
    rules = {}
    for rule in mapping_rules:
        rules[rule["source_field"]] = MappingRule(**rule)


def do_transform(
    source_data: list[Dict[str, Any]],
    mapping_rules: MappingRules,
    meta_data: CdmFileMetaData,
    cdm_model_def: CdmCompany = CdmCompany,
) -> List[Dict[str, Any]]:
    """
    map the source_data to the CDM model, return the CDM model in csv format
    """
    start = datetime.now()
    cdm_model = []

    for source_row in source_data:
        copy_of_mapping_rules = mapping_rules.rules.copy()
        row_data = get_dummy_cdm_model(meta_data, cdm_model_def)
        apply_cdm_model(source_row, row_data, copy_of_mapping_rules)
        if not is_metadata_set(row_data, meta_data):
            set_metadata(row_data, meta_data)
        cdm_model.append(row_data)

        del copy_of_mapping_rules
        del source_row
    logger.info(f"time taken: {(datetime.now() - start).total_seconds()}")
    return cdm_model


def schema_transformation(
    source_data: list[Dict[str, Any]],
    mapping_rules: MappingRules,
    meta_data: CdmFileMetaData,
    cdm_model_def: CdmCompany = CdmCompany,
    chunk_size: int = DEFAULT_MAX_DATA_CHUNK_SIZE,
) -> Generator[Tuple[List[Dict[str, Any]], int], None, None]:
    """
    map the source_data to the CDM model, return the CDM model in csv format
    """
    chunks = [
        source_data[i : i + chunk_size] for i in range(0, len(source_data), chunk_size)
    ]

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                do_transform, chunk, mapping_rules, meta_data, cdm_model_def
            ): i
            for i, chunk in enumerate(chunks)
        }

        for future in as_completed(futures):
            chunk_index = futures[future]
            chunk_result = future.result()
            yield chunk_result, chunk_index


def get_default_value(rule: MappingRule) -> Any:
    """
    Get the type of the value based on the rule.
    """
    if rule.value_type == ValueType.LIST:
        return []
    elif rule.value_type == ValueType.DICT:
        return {}
    elif rule.strategy == Strategy.LITERAL:
        return rule.literal_value
    else:
        return None


def get_dummy_cdm_model(
    meta_data: CdmFileMetaData | None = None,
    model_def: CdmCompany | OrderedEnum = CdmCompany,
) -> Dict[str, Any]:
    """
    build an empty cdm model based on the definition
    """
    # create a dictionary based on CdmCompany
    cdm_model = {}
    for field in model_def:
        cdm_model[field.value] = None
    if meta_data:
        cdm_model.update(meta_data.model_dump())
    return cdm_model


def get_rules_by_cdm_field(
    field_name: str, mapping_rules: Dict[str, MappingRule]
) -> list[MappingRule]:
    """
    Get the rules based on the cdm field
    """
    rules = []
    for _, rule in mapping_rules.items():
        if rule.cdm_field == field_name:
            rules.append(rule)
    return rules


def find_parent_rule(rules: list[MappingRule], rule: MappingRule) -> MappingRule:
    """
    find the parent rule based on the parent cdm_field
    """
    for a_rule in rules:
        if a_rule.cdm_parent is None:
            # top level node, no duplication
            return a_rule
        elif a_rule.cdm_field == rule.cdm_parent and str(a_rule.pattern_group) == str(
            rule.pattern_group
        ):
            return a_rule
    return None


def get_parent_rule(
    rule: MappingRule, all_rules: Dict[str, MappingRule]
) -> MappingRule:
    """
    find the parent rule based on the cdm_field
    """
    if rule.cdm_parent is None:
        return None
    rules = get_rules_by_cdm_field(rule.cdm_parent, all_rules)
    if not rules:
        raise KeyError(f" {rule.cdm_parent} is not defined in the mapping rules")
    parent_rule = find_parent_rule(rules, rule)
    if not parent_rule:
        raise KeyError(f"parent rule for {rule.cdm_field} is not found")
    return parent_rule


def get_parent_node(
    nodes: Dict[str, Any], rule: MappingRule, all_rules: Dict[str, MappingRule]
) -> Any:
    """
    use the rule to find current node, return it if it exists
    if not, find the parent node by using rule.cdm_parent and
    travese the tree
    """
    if rule.cdm_parent is None:
        # top level node
        if rule.cdm_field in nodes:
            return nodes
        else:
            nodes[rule.cdm_field] = {}
            return nodes
        # has parent, so need to find the parent node
    elif rule.cdm_parent in nodes:
        return nodes[rule.cdm_parent]
    else:
        parent_rule = get_parent_rule(rule, all_rules)
        return get_parent_node(nodes, parent_rule, all_rules)


def get_regex_node(
    nodes: Dict[str, Any], rule: MappingRule, all_rules: Dict[str, MappingRule]
) -> Any:
    """
    find the node based on the regex pattern in the mapping rules
    This is a a little hack because we use the regex pattern as the key
    to groupd and store the matched entity. The mapping rules doesn't
    define the key, so we need to find the key based on the pattern
    """

    for node_name, node in nodes.items():
        if isinstance(node, dict):
            if rule.cdm_field in node:
                return node[rule.cdm_field]
            elif rule.pattern_group in node:
                parent_rules = get_rules_by_cdm_field(rule.cdm_parent, all_rules)
                rules = [
                    r for r in parent_rules if r.pattern_group == rule.pattern_group
                ]
                parent_rule = find_parent_rule(rules, rule)

                if parent_rule is None:
                    # no more parent node, return the current node
                    return node[rule.pattern_group]
                else:
                    node[rule.pattern_group].update({rule.cdm_parent: {}})
                    return get_regex_node(nodes, parent_rule, all_rules)

        elif node == rule.pattern_group or node == rule.cdm_field:
            node = {}
            return node


def get_plain_value(
    rule: MappingRule, source_value: Any, rules: dict[str, MappingRule] | None = None
) -> Any:
    """
    Get the value based on the rule
    """
    if rule.value_type == ValueType.DATE:
        try:
            # TBC: this will pad 0s to the date string so to resuce not-compliant date string
            # e.g. 132021
            # Instead of throwing an error later down the modelling with date functions
            # which expects the date string to be in the format of 8 digits 0 padded
            # But if we can't 'rescue it, so be it. Let the schema transforamtion handle it as an invalid data
            date_str = datetime.strptime(
                source_value, rule.python_date_format
            ).strftime(rule.python_date_format)
            return {
                "value": date_str,
                "format": rule.date_format,
                "python_date_format": rule.python_date_format,
            }
        except ValueError:
            return {
                "value": source_value,
                "format": rule.date_format,
                "python_date_format": rule.python_date_format,
            }
    else:
        if rule.strategy == Strategy.DEFAULT or rule.strategy == Strategy.ADD:
            return source_value
        elif rule.strategy == Strategy.LITERAL:
            # remove_literal_rule(rules, rule)
            return rule.literal_value
        else:
            return None


def remove_literal_rule(rules: Dict[str, MappingRule], rule: MappingRule) -> None:
    """
    remove the rule from the rules if it is literal and has no source_field
    """
    if rule.strategy == Strategy.LITERAL and rule.source_field is None:
        del rules[rule.source_field]


def search_leaf(
    nodes: dict[str, any], rules: dict[str, MappingRule], func: callable
) -> None:
    for _, node in nodes.items():
        if isinstance(node, dict):
            func(node, rules)
        elif isinstance(node, list):
            for item in node:
                func(item, rules)


def apply_cdm_model(
    source_row: Dict[str, Any],
    cdm_record: Dict[str, Any],
    mapping_rules: Dict[str, MappingRule],
) -> None:
    """
    apply the cdm model to the data
    """
    # for each key-value pair in the data, find the corresponding cdm field and add the value
    for source_name, source_value in source_row.items():
        if source_value is None or source_value == "":
            continue

        def get_rules_by_source_field(source_name: str) -> list[MappingRule]:
            """
            Get the rules based on the source name or pattern match
            """
            rules = []
            for _, rule in mapping_rules.items():
                if rule.source_field and rule.source_field == source_name:
                    return [rule]

            if not source_name:
                return None

            for _, rule in mapping_rules.items():
                if rule.pattern is not None and (
                    rule.strategy == Strategy.REGEX
                    or rule.strategy == Strategy.FIND_REGEX_PARENT
                ):
                    pattern = re.compile(rule.pattern)

                    if pattern.match(source_name):
                        rules.append(rule)
            return rules

        def group_by_regex_match(
            nodes: Dict[str, Any], rule: MappingRule
        ) -> Dict[str, Any]:
            """
            filter and group source data by the regex pattern as the source_field
            """
            pattern = re.compile(rule.pattern)
            group_label = None
            match = pattern.match(source_name)
            if match:
                if match.groups():
                    group_label = match.group(1)
                if group_label:
                    update_pattern_group(group_label, rule)
                    if group_label not in nodes:
                        nodes[group_label] = {rule.cdm_field: source_value}
                    else:
                        nodes[group_label].update({rule.cdm_field: source_value})
                        # udpate the pattern group so that we can use it to find the node later
                        # search for "strategy": "find_regex_parent", "pattern_group": 1,
                        # and same cdm_parent

                    return nodes

        def update_pattern_group(group_label: str, rule: MappingRule) -> None:
            """
            update the pattern group in the mapping rules
            """
            # get all rules with strategy FIND_REGEX_PARENT
            rules = [
                r
                for _, r in mapping_rules.items()
                if r.strategy == Strategy.FIND_REGEX_PARENT
            ]

            for a_rule in rules:
                if str(a_rule.pattern_group) == str(rule.pattern_group):
                    a_rule.pattern_group = group_label

        def get_node(nodes: Dict[str, Any], rule: MappingRule) -> Any:
            if not rule.cdm_parent:
                # top level node
                if rule.cdm_field in nodes:
                    return nodes
                else:
                    nodes[rule.cdm_field] = {}
                    return nodes
            if isinstance(nodes, dict):
                for _, node in nodes.items():
                    if isinstance(node, dict):
                        if rule.cdm_field in node:
                            return node
                        elif rule.pattern_group in node:
                            return node
                        else:
                            return get_node(node, rule)
            if rule.pattern_group:
                if rule.pattern_group not in nodes[rule.cdm_parent]:
                    nodes[rule.cdm_parent][rule.pattern_group] = {}
                return nodes[rule.cdm_parent]

        def find_regex_node_by_pattern(nodes: Dict[str, Any], rule: MappingRule) -> Any:
            """
            find the node based on the regex pattern in the mapping rules
            This is a a little hack because we use the regex pattern as the key
            to groupd and store the matched entity. The mapping rules doesn't
            define the key, so we need to find the key based on the pattern
            """
            for node_name, node in nodes.items():
                if isinstance(node, dict):
                    if rule.pattern_group in node:
                        return node[rule.pattern_group]
                    else:
                        result = find_regex_node_by_pattern(node, rule)
                        if result:
                            return result
                elif node == rule.pattern_group:
                    node = {}
                    return node

        def add_non_literal_to_model(
            record: Dict[str, Any],
            source_key: str,
            mapping_rule: MappingRule = None,
        ) -> Any:
            """
            Recursively search for source_key in a nested dictionary and add the value
            """
            #
            if not mapping_rule:
                rules = get_rules_by_source_field(source_key)
                if not rules:
                    return None
                if len(rules) == 1:
                    rule = rules[0]
            else:
                rule = mapping_rule
            if rule.strategy == Strategy.LITERAL:
                return record

            if rule.strategy == Strategy.REGEX:
                # start searching from the immediate parent node
                if rule.cdm_parent:
                    parent_rule = get_parent_rule(rule, mapping_rules)
                    if parent_rule.cdm_field in record:
                        if not record[parent_rule.cdm_field]:
                            record[parent_rule.cdm_field] = {}
                        result = group_by_regex_match(
                            record[parent_rule.cdm_field], rule
                        )
                        return result
                    else:
                        # parent node is not in the nodes, keep finding the parent node
                        node = add_non_literal_to_model(
                            record, parent_rule.source_field, parent_rule
                        )
                        node[parent_rule.cdm_field].update(
                            {rule.cdm_field: source_value}
                        )
                else:
                    return group_by_regex_match(result, rule)
            elif rule.strategy == Strategy.FIND_REGEX_PARENT:
                if not rule.pattern_group:
                    raise KeyError(
                        f"Rule {rule.cdm_field} has errors, it defines FIND_REGEX_PARENT but missing pattern_group"
                    )
                # a FIND_REGEX_PARENT rule must have a parent
                if not rule.cdm_parent:
                    raise KeyError(
                        f"rule {rule.cdm_field} has errors, it defines FIND_REGEX_PARENT but missing parent"
                    )
                if rule.cdm_parent in record:
                    pattern_group_key = str(rule.pattern_group)
                    if not record[rule.cdm_parent]:
                        record[rule.cdm_parent] = {}
                    if pattern_group_key not in record[rule.cdm_parent]:
                        record[rule.cdm_parent].update({pattern_group_key: {}})
                    record[rule.cdm_parent][pattern_group_key].update(
                        {rule.cdm_field: {}}
                    )
                    return record[rule.cdm_parent][pattern_group_key]
                else:
                    parent_rule = get_parent_rule(rule, mapping_rules)
                    node = add_non_literal_to_model(
                        record, parent_rule.source_field, parent_rule
                    )
                    node[parent_rule.cdm_field].update({rule.cdm_field: source_value})
                    return record
            elif rule.strategy == Strategy.DEFAULT or rule.strategy == Strategy.ADD:
                if not isinstance(record, dict):
                    raise KeyError("nodes is not a dictionary")
                if rule.cdm_field in record:
                    if (
                        rule.source_field
                        and rule.source_field == source_key
                        and record[rule.cdm_field] is None
                    ):
                        record[rule.cdm_field] = get_plain_value(
                            rule, source_value, mapping_rules
                        )
                    return record

                elif rule.cdm_parent:
                    parent_rule = get_parent_rule(rule, mapping_rules)

                    if rule.cdm_parent in record:
                        if not record[rule.cdm_parent]:
                            record[rule.cdm_parent] = {}
                        record[rule.cdm_parent].update({rule.cdm_field: {}})
                        if rule.source_field and rule.source_field == source_key:
                            record[rule.cdm_parent][rule.cdm_field] = get_plain_value(
                                rule, source_value, mapping_rules
                            )
                        else:
                            return record[rule.cdm_parent]
                    elif parent_rule:
                        # parent node is not in the nodes, keep finding the parent node
                        node = add_non_literal_to_model(
                            record, parent_rule.source_field, parent_rule
                        )
                        if node:
                            node[parent_rule.cdm_field].update(
                                {
                                    rule.cdm_field: get_plain_value(
                                        rule, source_value, mapping_rules
                                    )
                                }
                            )

                else:
                    # look for child nodes
                    for _, node in record.items():
                        if isinstance(node, dict):
                            result = add_non_literal_to_model(node, source_key)
                        elif isinstance(node, list):
                            for item in node:
                                result = add_non_literal_to_model(item, source_key)

        def add_literal_to_model(
            nodes: Dict[str, Any], rules: dict[str, MappingRule]
        ) -> Any:
            """
            Recursively search for source_key in a nested dictionary and add the value
            """
            # start searching from the immediate parent node
            # if parent node found, add the literal value
            # if not, keep finding the parent node of nodes down the leaf nodes
            literal_rules = {
                k: r for k, r in rules.items() if r.strategy == Strategy.LITERAL
            }
            for rule_name, rule in literal_rules.items():
                if rule.cdm_parent and rule.pattern_group is None:
                    parent_rule = get_parent_rule(rule, rules)
                    if parent_rule.cdm_field in nodes:
                        if not nodes[parent_rule.cdm_field]:
                            nodes[parent_rule.cdm_field] = {}
                        nodes[parent_rule.cdm_field].update(
                            {rule.cdm_field: rule.literal_value}
                        )
                        # del rules[rule_name]
                        continue

                elif rule.pattern_group:
                    if rule.pattern_group in nodes:
                        nodes[rule.pattern_group].update(
                            {rule.cdm_field: rule.literal_value}
                        )
                        del rules[rule_name]
                    else:
                        # pattern_group is not in the nodes, keep finding the parent node down the leaf nodes
                        search_leaf(nodes, rules, add_literal_to_model)
                elif rule.cdm_field in nodes:
                    nodes[rule.cdm_field] = rule.literal_value
                    # del rules[rule_name]
                    continue
                else:
                    # parent node is not in the nodes, keep finding the parent node down the leaf nodes
                    search_leaf(nodes, rules, add_literal_to_model)

        add_non_literal_to_model(cdm_record, source_name)
        add_literal_to_model(cdm_record, mapping_rules)


def get_date_fields(mapping_rules: MappingRules) -> list[str]:
    """ "
    Get the date fields from the mapping rules
    :param mapping_rules: Mapping rules
    :return: list of date fields
    e.g.
                "cor_file_date": {
                        "cdm_parent": "incorporation_date",
                        "source_field": "COR_FILE_DATE",
                        "cdm_field": "file_date",
                        "value_type": "date",
                        "date_format": "MMDDYYYY",
                        "python_date_format": "%m%d%Y",
                        "strategy": "add"
                }
    """

    date_fields = set()
    for _, rule in mapping_rules.rules.items():
        if rule.value_type == ValueType.DATE:
            date_fields.add(rule.cdm_parent if rule.cdm_parent else rule.cdm_field)
    return list(date_fields)
