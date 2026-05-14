import unittest

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.ops.mapper.text.clean_email_mapper import CleanEmailMapper
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class CleanEmailMapperTest(DataJuicerTestCaseBase):

    def _run_clean_email(self, op, samples):
        dataset = Dataset.from_list(samples)
        dataset = dataset.map(op.process, batch_size=2)
                
        for data in dataset:
            self.assertEqual(data['text'], data['target'])

    def test_clean_email(self):

        samples = [{
            'text': 'happy day euqdh@cjqi.com',
            'target': 'happy day '
        }, {
            'text': '请问你是谁dasoidhao@1264fg.45om',
            'target': '请问你是谁dasoidhao@1264fg.45om'
        }, {
            'text': 'ftp://examplema-nièrdash@hqbchd.ckdhnfes.cds',
            'target': 'ftp://examplema-niè'
        }, {
            'text': '👊23da44sh12@46hqb12chd.ckdhnfes.comd.dasd.asd.dc',
            'target': '👊'
        }]
        op = CleanEmailMapper()
        self._run_clean_email(op, samples)

    def test_replace_email(self):

        samples = [{
            'text': 'happy day euqdh@cjqi.com',
            'target': 'happy day <EMAIL>'
        }, {
            'text': '请问你是谁dasoidhao@1264fg.45om',
            'target': '请问你是谁dasoidhao@1264fg.45om'
        }, {
            'text': 'ftp://examplema-nièrdash@hqbchd.ckdhnfes.cds',
            'target': 'ftp://examplema-niè<EMAIL>'
        }, {
            'text': '👊23da44sh12@46hqb12chd.ckdhnfes.comd.dasd.asd.dc',
            'target': '👊<EMAIL>'
        }]
        op = CleanEmailMapper(repl='<EMAIL>')
        self._run_clean_email(op, samples)


if __name__ == '__main__':
    unittest.main()
