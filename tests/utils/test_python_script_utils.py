import unittest

from data_juicer.utils.python_script_utils import PythonScriptRunner


class PythonScriptRunnerTest(unittest.TestCase):

    def test_runs_process_with_sample_and_context(self):
        runner = PythonScriptRunner(
            """
def process(sample, context):
    sample["user"] = context["ctx"]["userAccount"]
    sample["value"] = sample["value"] + 1
    return sample
"""
        )

        result = runner.run(
            {"value": 1},
            {"ctx": {"userAccount": "wangjianda.667"}},
        )

        self.assertEqual(
            result,
            {
                "value": 2,
                "user": "wangjianda.667",
            },
        )

    def test_process_can_use_script_imports(self):
        runner = PythonScriptRunner(
            """
import time

def process(sample, context):
    sample["ts"] = int(time.time() * 1000)
    return sample
"""
        )

        result = runner.run({}, {})

        self.assertIsInstance(result["ts"], int)

    def test_run_with_args_supports_review_entrypoint(self):
        runner = PythonScriptRunner(
            python_code=(
                "def review(value, row, context):\n"
                "    return value == row['state'], ''\n"
            ),
            entrypoint="review",
            require_dict_result=False,
        )

        result = runner.run_with_args(
            {"scene": "feed"},
            {"state": {"scene": "feed"}},
            {"operator": "code_review_mapper"},
        )

        self.assertEqual(result, (True, ""))

    def test_run_with_args_keeps_runtime_exception_message(self):
        runner = PythonScriptRunner(
            python_code=(
                "def review(value, row, context):\n"
                "    raise ValueError('review failed')\n"
            ),
            entrypoint="review",
            require_dict_result=False,
        )

        with self.assertRaisesRegex(ValueError, "review failed"):
            runner.run_with_args("value", {}, {})

    def test_rejects_missing_entrypoint(self):
        with self.assertRaisesRegex(ValueError, "python_code must define a callable process"):
            PythonScriptRunner("x = 1")

    def test_rejects_missing_review_entrypoint(self):
        with self.assertRaisesRegex(ValueError, "python_code must define a callable review"):
            PythonScriptRunner("x = 1", entrypoint="review", require_dict_result=False)

    def test_rejects_non_dict_result(self):
        runner = PythonScriptRunner(
            """
def process(sample, context):
    return 1
"""
        )

        with self.assertRaisesRegex(ValueError, "python_code result must be a dictionary"):
            runner.run({}, {})

    def test_rejects_non_json_serializable_result(self):
        runner = PythonScriptRunner(
            """
def process(sample, context):
    return {"bad": set([1])}
"""
        )

        with self.assertRaisesRegex(ValueError, "python_code result must be JSON serializable"):
            runner.run({}, {})


if __name__ == "__main__":
    unittest.main()
