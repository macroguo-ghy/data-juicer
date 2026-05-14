import unittest

from data_juicer.ops import OPERATORS, _op_module_index, load_builtin_ops
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class LoadOPsTest(DataJuicerTestCaseBase):
    def test_load_builtin_ops_indexes_op_name_aliases(self):
        self.assertIn("vlm_ray_vllm_engine_pipeline", _op_module_index())

        load_builtin_ops(["vlm_ray_vllm_engine_pipeline"])

        self.assertIn("vlm_ray_vllm_engine_pipeline", OPERATORS.modules)

    def test_load_builtin_ops_indexes_nested_mapper_modules(self):
        self.assertIn("clean_email_mapper", _op_module_index())
        self.assertIn(
            "data_juicer.ops.mapper.text.clean_email_mapper",
            _op_module_index()["clean_email_mapper"],
        )

        load_builtin_ops(["clean_email_mapper"])

        from data_juicer.ops.mapper import CleanEmailMapper

        self.assertIs(OPERATORS.modules["clean_email_mapper"], CleanEmailMapper)


if __name__ == '__main__':
    unittest.main()
