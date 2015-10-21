import unittest
import os
import yaml
import shutil
import datetime

from hugin.flowcell_status import FlowcellStatus, FC_STATUSES
from hugin.flowcells import HiseqXFlowcell

DEFAULT_CONFIG = "/Users/ekaterinastepanova/work/hugin/unittests/config.yaml"

class TestFlowcells(unittest.TestCase):

    def setUp(self):
        with open(DEFAULT_CONFIG) as config:
            self.config = (yaml.load(config) or {})

        self.data_folder = self.config.get('data_folders')[0]
        self.fake_flowcell = os.path.join(self.data_folder,'151021_ST-E00144_0013_ABCDYCCXX')
        os.mkdir(self.fake_flowcell)

    def test_status_sequencing(self):
        fc_status = FlowcellStatus(self.data_folder)
        self.assertEqual(fc_status.status, FC_STATUSES['SEQUENCING'])

    def test_status_demultiplexing(self):
        filename = os.path.join(self.fake_flowcell, 'Demultiplexing')
        os.mkdir(filename)
        fc_status = FlowcellStatus(self.fake_flowcell)

        self.assertEqual(fc_status.status, FC_STATUSES['DEMULTIPLEXING'])
        os.rmdir(filename)

    def test_due_date(self):
        cycle_times_path = "/Users/ekaterinastepanova/nosync/hiseqX/150922_ST-E00214_0062_AH5VM7CCXX/Logs/CycleTimes.txt"
        logs_dir = os.path.join(self.fake_flowcell, 'Logs')
        os.mkdir(logs_dir)
        shutil.copy2(cycle_times_path, logs_dir)

        run_info_path = "/Users/ekaterinastepanova/nosync/hiseqX/150922_ST-E00214_0062_AH5VM7CCXX/RunInfo.xml"
        shutil.copy2(run_info_path, self.fake_flowcell)

        due_date = datetime.datetime(2015, 8, 28, 3, 0, 51, 405770)

        fc_status = FlowcellStatus(self.fake_flowcell)
        fc = HiseqXFlowcell(fc_status)
        self.assertEqual(due_date, fc.due_time)

        os.remove(os.path.join(self.fake_flowcell, 'RunInfo.xml'))
        shutil.rmtree(logs_dir)

    def test_check_status(self):
        cycle_times_path = "/Users/ekaterinastepanova/nosync/hiseqX/150922_ST-E00214_0062_AH5VM7CCXX/Logs/CycleTimes.txt"
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