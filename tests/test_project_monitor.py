import unittest
import tempfile
import os
import yaml
import shutil
import trello
from hugin.project_monitor import ProjectMonitor
        
class TestRunMonitor(unittest.TestCase):
    
    def setUp(self):
        self.dump_folder = tempfile.mkdtemp(prefix="test_project_monitor_")
        
        cfile = os.path.expanduser(os.path.join("~",".hugin","hugin_test_conf.yaml"))
        with open(cfile) as fh:
            self.config = yaml.load(fh)
            
        self.config['run_folders'] = self.dump_folder
        
    def tearDown(self):
        shutil.rmtree(self.dump_folder)
        

    def test_add_project_card(self):
        """Add a project card to project board"""
        
        pm = ProjectMonitor(self.config)
        project = "J.Doe_11_01"
        card = pm.add_project_card(project)
        card.fetch()
        self.assertIs(type(card),
                      trello.Card,
                      "Did not get a Card object back")
        
        self.assertFalse(card.closed,
                         "The added card is closed")
        
        lst = pm.trello.get_list(pm.trello_board, "Sequencing")
        self.assertEqual(card.list_id,
                         lst.id,
                         "Card was not added to the expected list")
        
        run = {'name': "Test run 001"}
        pm.add_run_to_project(project, run)
        
        run = {'name': "Test run 002"}
        pm.add_run_to_project(project, run)