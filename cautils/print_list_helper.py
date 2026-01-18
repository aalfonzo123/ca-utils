from typing import Any, Union
from rich.table import Table
from rich import box

DEFAULT_VALUE = "N.A."


def after_last_slash(v):
    """Extracts the part of a string after the last slash.

    Args:
        v (str): The input string, typically a file path.

    Returns:
        str: The substring after the last occurrence of '/'.
    """
    return v.split("/")[-1]


def after_last_slash_multi(v: list[str]) -> str:
    """Extracts the part of a string after the last slash for multiple strings.

    Args:
        v (list[str]): A list of input strings, typically file paths.

    Returns:
        str: A comma-separated string of substrings after the last occurrence of '/'.
    """
    return ", ".join([elem.split("/")[-1] for elem in v])


def _get_safe_single_path(original_obj: Any, path_str: str) -> Any:
    """Safely retrieves a value from a nested object using a dot-separated path.

    This function traverses an object (or dictionary) based on the provided path
    string. If any part of the path is not found, it returns None.

    Args:
        original_obj: The object or dictionary to traverse.
        path_str: A dot-separated string representing the path to the desired value
                  (e.g., "metadata.name", "spec.template.spec.containers[0].image").

    Returns:
        The value found at the specified path, or None if any part of the path
        does not exist or is not accessible.
    """
    obj = original_obj
    if not path_str:
        return obj

    for element in path_str.split("."):
        if hasattr(obj, element):
            obj = getattr(obj, element)
        elif isinstance(obj, dict) and element in obj:
            obj = obj[element]
        else:
            return None
    return obj


def _get_safe(obj: Any, path: Union[str, list[str]], base_path: str = "") -> Any:
    """Safely retrieves a value or values from a nested object.

    This function allows fetching data from deeply nested structures (objects or dictionaries)
    using a dot-separated path string. It supports retrieving a single value or multiple
    values by providing a list of paths.

    Args:
        obj: The object (or dictionary) to traverse.
        path: The path(s) to the desired value(s), separated by dots. Can be a single
              string (e.g., "field.subfield") or a list of strings.
        base_path: An optional base path string to prepend to all `path` elements.
                   Useful for common prefixes.

    Returns:
        If `path` is a string: The value found at the specified path, or `None` if not found.
        If `path` is a list of strings: A dictionary where keys are the original paths
        and values are the results of looking for each path in the object (`None` if not found).
        Returns `None` if `path` is neither a string nor a list.
    """
    base_path = (
        base_path if base_path.endswith(".") or base_path == "" else base_path + "."
    )
    if isinstance(path, str):
        return _get_safe_single_path(obj, base_path + path)
    elif isinstance(path, list):
        result_dict = {}
        for p_str in path:
            result_dict[p_str] = _get_safe_single_path(obj, base_path + p_str)
        return result_dict
    else:
        return None


def get_table_generic(data: dict[str, list[Any]], col_specs: dict) -> Table:
    """Generates a rich.Table object with data formatted according to column specifications.

    This function dynamically creates a table based on provided column headers and
    specifications for extracting data for each row from a given data source.

    Args:
        data: A dictionary where keys are string identifiers and values are lists of
              items (dictionaries or objects) to be displayed in the table. Each item
              in the list represents a row.
        col_specs: A dictionary defining column headers and their specifications.
                   Keys are the column headers (str). Values can be either:
                   - A `str`: representing a dot-separated path to a value within each item in `data`
                     (e.g., "name", "spec.displayName").
                   - A `dict`: containing the following keys:
                     - `path` (str, required): Dot-separated path to the value.
                     - `opts` (dict, optional): Keyword arguments for `table.add_column`
                       (e.g., `{"style": "bold magenta"}`).
                     - `proc` (Callable, optional): A function to transform the found value.
                     - `default` (Any, optional): Default value to display if path not found.

    Returns:
        rich.table.Table: A formatted rich Table object ready for printing.
    """

    table = Table(box=box.SQUARE, show_lines=True)
    for name, spec in col_specs.items():
        opts = spec.get("opts", {}) if isinstance(spec, dict) else {}
        table.add_column(name, **opts)

    for item in data or []:
        result = []
        for spec in col_specs.values():
            if isinstance(spec, dict):
                default = spec.get("default", DEFAULT_VALUE)
                value = _get_safe(item, spec["path"], spec.get("base_path", ""))
                if value and spec.get("proc"):
                    value = spec["proc"](value)
                result.append(value or default)
            else:
                result.append(_get_safe(item, spec) or DEFAULT_VALUE)

        # print(result)
        table.add_row(*tuple(result))

    return table
