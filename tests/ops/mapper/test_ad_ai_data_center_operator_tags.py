import importlib
import pkgutil
import unittest

from data_juicer.ops.mapper import ad_ai_data_center


class AdAiDataCenterOperatorTagTest(unittest.TestCase):
    @staticmethod
    def _operator_module_names():
        return [
            module_info.name
            for module_info in pkgutil.iter_modules(ad_ai_data_center.__path__)
            if not module_info.ispkg and not module_info.name.startswith("_")
        ]

    def test_all_ad_ai_data_center_mappers_define_operator_tag(self):
        module_names = self._operator_module_names()
        self.assertGreater(len(module_names), 0)
        for module_name in module_names:
            with self.subTest(module=module_name):
                module = importlib.import_module(f"{ad_ai_data_center.__name__}.{module_name}")
                self.assertEqual(module.OPERATOR_TAG, "business_operator")

    def test_all_ad_ai_data_center_mappers_define_display_name(self):
        module_names = self._operator_module_names()
        self.assertGreater(len(module_names), 0)
        for module_name in module_names:
            with self.subTest(module=module_name):
                module = importlib.import_module(f"{ad_ai_data_center.__name__}.{module_name}")
                self.assertIsInstance(module.OP_DISPLAY_NAME, str)
                self.assertTrue(module.OP_DISPLAY_NAME)


if __name__ == "__main__":
    unittest.main()
