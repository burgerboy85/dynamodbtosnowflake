# utils/flatten.py

from typing import Any, Dict, Mapping, Union


def flatten_json(
    data: Union[Mapping[str, Any], list],
    parent_key: str = "",
    sep: str = "_"
) -> Dict[str, Any]:
    """
    Recursively flattens a dict or list into a single dict of dotted/underscore keys.

    Examples:
        {"a": {"b": 1}}    → {"a_b": 1}
        {"x": [ {"y": 2} ]} → {"x_0_y": 2}

    :param data: nested dict or list
    :param parent_key: prefix for keys (used internally)
    :param sep: separator between key segments
    :return: flattened dict
    """
    items: Dict[str, Any] = {}

    if isinstance(data, Mapping):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.update(flatten_json(value, new_key, sep=sep))
    elif isinstance(data, list):
        for idx, value in enumerate(data):
            new_key = f"{parent_key}{sep}{idx}" if parent_key else str(idx)
            items.update(flatten_json(value, new_key, sep=sep))
    else:
        items[parent_key] = data

    return items
