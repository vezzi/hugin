import unittest
import os
import yaml
import shutil
import datetime

from hugin.flowcell_status import FlowcellStatus, FC_STATUSES
from hugin.flowcells import HiseqXFlowcell

DEFAULT_CONFIG = "test_data/config.yaml"

class TestFlowcells(unittest.TestCase):

    def setUp(self):
        with open(DEFAULT_CONFIG) as config:
            self.config = (yaml.load(config) or {})
        self.original_data_folder = os.path.join('tests' , self.config.get('data_folders')[0])
        self.fake_data_folder     = os.path.join('test_data')
        self.original_flowcell = os.path.join(self.original_data_folder , '150424_ST-E00214_0031_BH2WY7CCXX')
        self.fake_flowcell     = os.path.join(self.fake_data_folder     , '150424_ST-E00214_0031_BH2WY7CCXX')
        os.mkdir(self.fake_flowcell)

    def test_status_sequencing(self):
        fc_status = FlowcellStatus(self.fake_data_folder)
        self.assertEqual(fc_status.status, FC_STATUSES['SEQUENCING'])

    def test_status_demultiplexing(self):
        filename = os.path.join(self.fake_flowcell, 'Demultiplexing')
        os.mkdir(filename)
        fc_status = FlowcellStatus(self.fake_flowcell)

        self.assertEqual(fc_status.status, FC_STATUSES['DEMULTIPLEXING'])
        os.rmdir(filename)

    def test_due_date(self):
        cycle_times_path = os.path.join(self.original_flowcell, 'Logs', 'CycleTimes.txt')
        logs_dir = os.path.join(self.fake_flowcell, 'Logs')
        os.mkdir(logs_dir)
        shutil.copy2(cycle_times_path, logs_dir)

        run_info_path = os.path.join(self.original_flowcell, 'RunInfo.xml')
        shutil.copy2(run_info_path, self.fake_flowcell)

        due_date = datetime.datetime(2015, 10, 9, 3, 10, 23, 707830)

        fc_status = FlowcellStatus(self.fake_flowcell)
        fc = HiseqXFlowcell(fc_status)
        self.assertEqual(due_date, fc.due_time)

        os.remove(os.path.join(self.fake_flowcell, 'RunInfo.xml'))
        shutil.rmtree(logs_dir)

    def test_check_status(self):
        cycle_times_path = os.path.join(self.original_flowcell, 'Logs', 'CycleTimes.txt')
        logs_dir = os.path.join(self.fake_flowcell, 'Logs')
        os.mkdir(logs_dir)
        shutil.copy2(cycle_times_path, logs_dir)

        fc_status = FlowcellStatus(self.fake_flowcell)
        fc = HiseqXFlowcell(fc_status)
        self.assertTrue(fc.check_status())
        shutil.rmtree(logs_dir)


    def tearDown(self):
        shutil.rmtree(self.fake_flowcell, ignore_errors=False)


if __name__ == '__main__':
    unittest.main()