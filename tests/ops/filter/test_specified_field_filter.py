import unittest

from data_juicer.core.data import NestedDataset as Dataset

from data_juicer.ops.filter.specified_field_filter import SpecifiedFieldFilter
from data_juicer.utils.constant import Fields
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class SpecifiedFieldFilterTest(DataJuicerTestCaseBase):

    def _run_specified_field_filter(self, dataset: Dataset, target_list, op):
        if Fields.stats not in dataset.features:
            dataset = dataset.add_column(name=Fields.stats,
                                         column=[{}] * dataset.num_rows)
        dataset = dataset.map(op.compute_stats)
        dataset = dataset.filter(op.process)
        dataset = dataset.remove_columns(Fields.stats)
        res_list = dataset.to_list()
        self.assertEqual(res_list, target_list)

    def test_case(self):

        ds_list = [{
            'text': 'Today is Sun',
            'meta': {
                'suffix': '.pdf',
                'star': 50
            }
        }, {
            'text': 'a v s e c s f e f g a a a  ',
            'meta': {
                'suffix': '.docx',
                'star': 6
            }
        }, {
            'text': '中文也是一个字算一个长度',
            'meta': {
                'suffix': '.txt',
                'star': 100
            }
        }, {
            'text': '，。、„”“«»１」「《》´∶：？！',
            'meta': {
                'suffix': '',
                'star': 12.51
            }
        }, {
            'text': 'dasdasdasdasdasdasdasd',
            'meta': {
                'suffix': None
            }
        }]
        tgt_list = [{
            'text': 'Today is Sun',
            'meta': {
                'suffix': '.pdf',
                'star': 50
            }
        }, {
            'text': '中文也是一个字算一个长度',
            'meta': {
                'suffix': '.txt',
                'star': 100
            }
        }]
        dataset = Dataset.from_list(ds_list)
        op = SpecifiedFieldFilter(field_key='meta.suffix',
                                  target_value=['.pdf', '.txt'])
        self._run_specified_field_filter(dataset, tgt_list, op)

    def test_list_case(self):

        ds_list = [{
            'text': 'Today is Sun',
            'meta': {
                'suffix': '.pdf',
                'star': 50,
                'path': {
                    'test': ['txt', 'json'],
                    'test2': 'asadd'
                }
            }
        }, {
            'text': 'a v s e c s f e f g a a a  ',
            'meta': {
                'suffix': '.docx',
                'star': 6,
                'path': {
                    'test': ['pdf', 'txt', 'xbs'],
                    'test2': ''
                }
            }
        }, {
            'text': '中文也是一个字算一个长度',
            'meta': {
                'suffix': '.txt',
                'star': 100,
                'path': {
                    'test': ['docx', '', 'html'],
                    'test2': 'abcd'
                }
            }
        }, {
            'text': '，。、„”“«»１」「《》´∶：？！',
            'meta': {
                'suffix': '',
                'star': 12.51,
                'path': {
                    'test': ['json'],
                    'test2': 'aasddddd'
                }
            }
        }, {
            'text': 'dasdasdasdasdasdasdasd',
            'meta': {
                'suffix': None,
                'star': 333,
                'path': {
                    'test': ['pdf', 'txt', 'json', 'docx'],
                    'test2': None
                }
            }
        }]
        tgt_list = [{
            'text': 'Today is Sun',
            'meta': {
                'suffix': '.pdf',
                'star': 50,
                'path': {
                    'test': ['txt', 'json'],
                    'test2': 'asadd'
                }
            }
        }, {
            'text': '，。、„”“«»１」「《》´∶：？！',
            'meta': {
                'suffix': '',
                'star': 12.51,
                'path': {
                    'test': ['json'],
                    'test2': 'aasddddd'
                }
            }
        }]
        dataset = Dataset.from_list(ds_list)
        op = SpecifiedFieldFilter(field_key='meta.path.test',
                                  target_value=['pdf', 'txt', 'json'])
        self._run_specified_field_filter(dataset, tgt_list, op)

    def test_process_single_falls_back_to_original_field_when_stats_missing(self):
        op = SpecifiedFieldFilter(field_key='qwen_postprocess_error',
                                  target_value=[''])

        self.assertTrue(op.process_single({
            Fields.stats: {},
            'qwen_postprocess_error': '',
        }))
        self.assertFalse(op.process_single({
            Fields.stats: {},
            'qwen_postprocess_error': 'invalid_json',
        }))

    def test_process_single_falls_back_to_nested_original_field_when_stats_missing(self):
        op = SpecifiedFieldFilter(field_key='meta.suffix',
                                  target_value=['.pdf'])

        self.assertTrue(op.process_single({
            Fields.stats: {},
            'meta': {
                'suffix': '.pdf',
            },
        }))
        self.assertFalse(op.process_single({
            Fields.stats: {},
            'meta': {
                'suffix': '.docx',
            },
        }))


if __name__ == '__main__':
    unittest.main()
