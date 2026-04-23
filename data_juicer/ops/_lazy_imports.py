from __future__ import annotations

import ast
import importlib
import warnings
from functools import lru_cache
from pathlib import Path


def build_class_index(package_file: str) -> dict[str, str]:
    package_root = Path(package_file).resolve().parent
    index: dict[str, str] = {}
    for module_path in package_root.rglob("*.py"):
        if module_path.name == "__init__.py" or module_path.name.startswith("_"):
            continue
        rel_module = module_path.relative_to(package_root).with_suffix("")
        module_name = "." + ".".join(rel_module.parts)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SyntaxWarning)
                tree = ast.parse(module_path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.ClassDef):
                index[node.name] = module_name
    return index


def make_class_index(package_file: str):
    @lru_cache(maxsize=1)
    def _class_index() -> dict[str, str]:
        return build_class_index(package_file)

    return _class_index


def load_class(package_name: str, class_index, name: str):
    module_name = class_index().get(name)
    if module_name is None:
        raise AttributeError(f"module '{package_name}' has no attribute '{name}'")

    value = getattr(importlib.import_module(module_name, package_name), name)
    return value
