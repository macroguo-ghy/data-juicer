from __future__ import annotations

import json
from typing import Any


class PythonScriptRunner:
    """Compile and run a trusted Python script entrypoint.

    This is intentionally not a sandbox. The script runs in the current Python
    process with normal builtins and should only be used for trusted code.
    """

    def __init__(
        self,
        python_code: str,
        entrypoint: str = "process",
        require_dict_result: bool = True,
    ):
        if not python_code:
            raise ValueError("python_code must be provided")
        if not entrypoint:
            raise ValueError("entrypoint must be provided")

        self.python_code = python_code
        self.entrypoint = entrypoint
        self.require_dict_result = require_dict_result
        self.process_func = self._compile_process_func(python_code, entrypoint)

    def run(self, data: Any, context: dict[str, Any] | None = None):
        return self.run_with_args(data, context or {})

    def run_with_args(self, *args):
        result = self.process_func(*args)
        if self.require_dict_result and not isinstance(result, dict):
            raise ValueError(
                "python_code result must be a dictionary, "
                f"got {type(result).__name__} instead."
            )
        self._ensure_json_serializable(result)
        return result

    @staticmethod
    def _compile_process_func(python_code: str, entrypoint: str):
        namespace: dict[str, Any] = {"__builtins__": __builtins__}
        try:
            compiled_code = compile(python_code, "<python_script_runner>", "exec")
            # Use one namespace for globals and locals so imports and helper
            # functions are visible from the configured entrypoint.
            exec(compiled_code, namespace, namespace)
        except Exception as exc:
            raise ValueError(f"Invalid python_code: {exc}") from exc

        process_func = namespace.get(entrypoint)
        if not callable(process_func):
            raise ValueError(f"python_code must define a callable {entrypoint}")
        return process_func

    @staticmethod
    def _ensure_json_serializable(value):
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"python_code result must be JSON serializable: {exc}") from exc
