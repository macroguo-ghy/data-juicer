import unittest

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops.mapper.text.remove_long_words_mapper import \
    RemoveLongWordsMapper
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class RemoveLongWordsMapperTest(DataJuicerTestCaseBase):

    def _run_remove_long_words(self, samples, op):
        dataset = Dataset.from_list(samples)
        dataset = dataset.map(op.process, batch_size=2)
                
        for data in dataset:
            self.assertEqual(data['text'], data['target'])

    def test_normal_case(self):

        samples = [{
            'text':
            'This paper proposed novel method LLM pretraining.',
            'target':
            'This paper proposed novel method LLM pretraining.'
        }]
        op = RemoveLongWordsMapper(min_len=3, max_len=15)
        self._run_remove_long_words(samples, op)

    def test_long_short_words_case(self):

        samples = [{
            'text':
            'This paper a novel eqeqweqwewqeqwe121e1 method on LLM pretrain.',
            'target': 'This paper novel method LLM pretrain.'
        }, {
            'text':
            'Sur la plateforme MT4, manières à ces fonctionnalités sont conçu',
            'target':
            'Sur plateforme MT4, manières ces fonctionnalités sont conçu'
        }]
        op = RemoveLongWordsMapper(min_len=3, max_len=15)
        self._run_remove_long_words(samples, op)

    def test_special_words_case(self):

        samples = [{
            'text':
            'This paper proposed a novel eqeqweqwewqenhq😊😠 method on LLM.',
            'target':
            'This paper proposed novel eqeqweqwewqenhq😊😠 method LLM.'
        }, {
            'text':
            "Sur la plateforme MT4, plusieurs manières d'accéder0123813976125",
            'target':
            "Sur plateforme MT4, plusieurs manières d'accéder0123813976125"
        }, {
            'text': 'The Mona Lisa doesnÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢t have eyebrows.',
            'target': 'The Mona Lisa have eyebrows.'
        }]
        op = RemoveLongWordsMapper(min_len=3, max_len=15)
        self._run_remove_long_words(samples, op)


if __name__ == '__main__':
    unittest.main()
