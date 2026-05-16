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

    def test_rejects_missing_entrypoint(self):
        with self.assertRaisesRegex(ValueError, "python_code must define a callable process"):
            PythonScriptRunner("x = 1")

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
