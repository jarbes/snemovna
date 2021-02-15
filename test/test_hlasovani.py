import unittest

from Hlasovani import *

class TestHlasovani(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # print('setupClass')
        cls.hlasovani = Hlasovani(volebni_obdobi=2017, data_dir="./data", stahni=False)
        pass

    @classmethod
    def tearDownClass(cls):
        # print('teardownClass')
        pass

    def setUp(self):
        print('setUp')

    def tearDown(self):
        # print('tearDown')
        pass

    def test_nacti_hlasovani(self):
        print(self.hlasovani.df.head())
        #self.assertEqual(self.df.fit(), self.train_accuracy) 
        #self.assertEqual(self.ta.train_confusion_matrix.tolist(), self.train_confusion_matrix.tolist())  

if __name__ == '__main__':
    #run tests
    unittest.main()

