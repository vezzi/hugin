import unittest
import tempfile
import os
import shutil
import trello
from hugin.run_monitor import RunMonitor

class TestRunMonitor(unittest.TestCase):
    
    def setUp(self):
        self.dump_folder = tempfile.mkdtemp(prefix="test_run_monitor_")
        self.config = {'trello': {'api_key': '35c3947807caa06935842db61619a1c3',
                                  'token': '1846c76339bc24903f71a55ed522d2855fb94c148f2690c4553e50be6ea1baaf',
                                  'api_secret': '64fafa5ca6fa0b190c7322b4166b199d0b734775ac6e8e2b9ca0077b3675dd33',
                                  'test_board': 'test_board',
                                  'test_board_id': '517082eaf79e031b2a001e51',
                                  'test_list': 'test_list',
                                  'test_card': 'test_card',
                                  'run_tracking_board': 'test_board'
                                  },
                       'run_folders': self.dump_folder
                       }
        
    def tearDown(self):
        shutil.rmtree(self.dump_folder)
        
    def test_list_runs(self):
        """List run folders"""
        run_folders = ["120106_SN12345_0144_AABC123CXX",
                       "120521_M00123_0001_AFCGHY76-KTY500"]
        invalid_run_folders = ["ABC_SN123_0123_BASDC34CXX",
                               "120106_SN12345_0144_CABC123CXX",
                               "120521_K00123_0001_AFCGHY76-KTY500"]
        for d in run_folders + invalid_run_folders:
            os.mkdir(os.path.join(self.dump_folder,d))
        
        rm = RunMonitor(self.config)
        runs = rm.list_runs()
        self.assertListEqual(sorted(run_folders),
                             sorted([r['name'] for r in runs]),
                             "Did not return expected list of runs")
        
        for d in run_folders + invalid_run_folders:
            shutil.rmtree(os.path.join(self.dump_folder,d))
        
    def test_update_trello_board(self):
        """Update Trello list with runs"""
        
        run_folders = ["120106_SN12345_0144_AABC123CXX",
                       "120521_M00123_0001_AFCGHY76-KTY500"]
        for d in run_folders:
            os.mkdir(os.path.join(self.dump_folder,d))
        
        rm = RunMonitor(self.config)
        rm.update_trello_board()
        lst = rm.trello.get_list(rm.trello_board,'First read')
        for run in run_folders:
            card = rm.trello.get_card(lst,run)
            self.assertIsNotNone(card,
                                 "Could not locate created card for run")
            self.assertEqual(run,
                             card.name,
                             "Created card name and run name are not equal")
            shutil.rmtree(os.path.join(self.dump_folder,run))
            