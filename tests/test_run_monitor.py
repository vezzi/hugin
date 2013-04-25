import unittest
import tempfile
import os
import yaml
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
        
        cfile = os.path.expanduser(os.path.join("~",".hugin","hugin_test_conf.yaml"))
        with open(cfile) as fh:
            self.config = yaml.load(fh)
            
        self.config['run_folders'] = self.dump_folder
        
    def tearDown(self):
        shutil.rmtree(self.dump_folder)
        
    def test_list_runs(self):
        """List run folders"""
        run_folders = ["120106_SN12345_0144_AABC123CXX",
                       "120521_M00123_0001_AFCGHY76-KTY500",
                       "130423_D00134_0011_AD1Y4UACXX"]
        invalid_run_folders = ["ABC_SN123_0123_BASDC34CXX",
                               "120106_SN12345_0144_CABC123CXX",
                               "120521_K00123_0001_AFCGHY76-KTY500"]
        for d in run_folders + invalid_run_folders:
            os.mkdir(os.path.join(self.dump_folder,d))
        
        rm = RunMonitor(self.config)
        rm.get_run_info = mock.Mock(return_value={})
        rm.get_run_parameters = mock.Mock(return_value={})
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
        rm.get_run_info = mock.Mock(return_value={'Reads': [{'NumCycles': 50},{'IsIndexedRead': 'Y'},{'NumCycles': 50}]})
        rm.get_run_parameters = mock.Mock(return_value={})
        
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
                         "Check status",
                         "Expected status list 'Check status'")
        
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
        
    def test_parse_description(self):
        """Parse a description"""
        
        org_description = \
"""- extra comment
- project:J.Doe_11_02,J.Doe_12_01,J.Doe_13_01
- setup:2x101bp""".strip()
        
        rm = RunMonitor(self.config)
        org_metadata = rm.parse_description(org_description)
        exp_description = rm.create_description(org_metadata)
        exp_metadata = rm.parse_description(exp_description)
        
        self.assertEqual(org_description,
                         exp_description,
                         "The recreated card description does not match the original")
        
        self.assertDictEqual(org_metadata,
                             exp_metadata,
                             "The recreated card metadata does not match the original")
        
    def test_send_notification(self):
        """Send an email notification"""
        
        rm = RunMonitor(self.config)
        exception = None
        try:
            rm.send_notification({'name': "test run"},"dummy value")
        except Exception, e: 
            exception = e
        
        self.assertIsNone(exception,
                          "Sending email raised exception {}".format(str(exception)))
        
        
        