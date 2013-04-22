import unittest
import tempfile
import os
import shutil
import trello
from hugin.run_monitor import RunMonitor
import mock
import time
import datetime
import scilifelab.illumina as illumina
from scilifelab.bcbio.qc import FlowcellRunMetricsParser
        
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
        rm.get_run_projects = mock.Mock(return_value=['J.Doe_11_01','J.Moe_12_02'])
        rm.get_status_list = mock.Mock(return_value='First read')
        
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
            
        # Move a card to another list
        run = {'name': run_folders[0]}
        rm.list_runs = mock.Mock(return_value=[run])
        rm.get_status_list.return_value = 'Index read'
        rm.update_trello_board()
        lst = rm.trello.get_list(rm.trello_board,'Index read')
        card = rm.trello.get_card(lst,run['name'])
        self.assertIsNotNone(card,
                             "Could not locate created card for run")
        self.assertEqual(run['name'],
                         card.name,
                         "Created card name and run name are not equal")
        
    
    def test_get_status_list(self):
        """Get the status list to write card to"""
        
        run_folder = "120106_SN12345_0144_AABC123CXX"
        run_path = os.path.join(self.dump_folder,run_folder)
        os.mkdir(run_path)
        
        flags = ['initial_processing_started.txt', 'initial_processing_completed.txt', 'first_read_processing_started.txt']
        for f in flags:
            with open(os.path.join(run_path,f),"w") as fh:
                fh.write("{}Z".format(str(datetime.datetime.utcfromtimestamp(time.time() - 9*60*60))))
                
        run = {'name': run_folder,
               'path': run_path}
        
        rm = RunMonitor(self.config)
        rm.get_run_info = mock.Mock(return_value={'Reads': [{},{'IsIndexedRead': 'Y'},{'IsIndexedRead': 'Y'},{}]})
        self.assertEqual(rm.get_status_list(run),
                         "Stalled - check status",
                         "Expected status list 'Stalled - check status'")
        
        os.unlink(os.path.join(run_path,flags[-1]))
        
        self.assertEqual(rm.get_status_list(run),
                         "First read",
                         "Expected status list 'First read'")
        
        open(os.path.join(run_path,'Basecalling_Netcopy_complete_Read1.txt'),'w').close()
        self.assertEqual(rm.get_status_list(run),
                         "Index read",
                         "Expected status list 'Index read'")
        
        open(os.path.join(run_path,'Basecalling_Netcopy_complete_Read2.txt'),'w').close()
        self.assertEqual(rm.get_status_list(run),
                         "Index read",
                         "Expected status list 'Index read'")
        
        open(os.path.join(run_path,'Basecalling_Netcopy_complete_Read3.txt'),'w').close()
        self.assertEqual(rm.get_status_list(run),
                         "Second read",
                         "Expected status list 'Second read'")
        
        open(os.path.join(run_path,'Basecalling_Netcopy_complete_Read4.txt'),'w').close()
        self.assertEqual(rm.get_status_list(run),
                         "Processing",
                         "Expected status list 'Processing'")
        
        shutil.rmtree(run_path)
        
        
        
        