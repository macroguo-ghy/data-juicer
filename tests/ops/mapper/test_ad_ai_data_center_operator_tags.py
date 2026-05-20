import importlib
import pkgutil
import unittest

from data_juicer.ops.mapper import ad_ai_data_center


class AdAiDataCenterOperatorTagTest(unittest.TestCase):
    def test_all_ad_ai_data_center_mappers_define_operator_tag(self):
        module_names = [
            module_info.name
            for module_info in pkgutil.iter_modules(ad_ai_data_center.__path__)
            if module_info.name.endswith("_mapper")
        ]

        self.assertGreater(len(module_names), 0)
        for module_name in module_names:
            with self.subTest(module=module_name):
                module = importlib.import_module(f"{ad_ai_data_center.__name__}.{module_name}")
                self.assertEqual(module.OPERATOR_TAG, "business_operator")


if __name__ == "__main__":
    unittest.main()
