import unittest

import pandas as pd

from snemovna.Hlasovani import Hlasovani

class TestHlasovani(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('setUpClass')
        cls.hlasovani = Hlasovani(volebni_obdobi=2017, data_dir="./data", stahni=False)
        pass

    @classmethod
    def tearDownClass(cls):
        # print('teardownClass')
        pass

    def setUp(self):
        # print('setUp')
        pass

    def tearDown(self):
        # print('tearDown')
        pass

    def test_nacti_hlasovani(self):
        ret = self.hlasovani.nacti_hlasovani()
        self.assertIsInstance(ret, tuple)
        self.assertEqual(len(ret), 2)
        self.assertIsInstance(ret[0], pd.core.frame.DataFrame)
        self.assertIsInstance(ret[1], pd.core.frame.DataFrame)

if __name__ == '__main__':
    unittest.main()

