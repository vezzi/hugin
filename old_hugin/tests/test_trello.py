import unittest
import os

import trello
import yaml
from old_hugin.trello_utils import TrelloUtils


class TestTrelloUtils(unittest.TestCase):
    
    def setUp(self):
        cfile = os.path.expanduser(os.path.join("~",".hugin","hugin_test_conf.yaml"))
        with open(cfile) as fh:
            self.config = yaml.load(fh)
        self.trello = TrelloUtils(self.config)
        
    def tearDown(self):
        pass
    
    def test_client(self):
        """Initiatializing TrelloClient"""
        self.assertIs(type(self.trello.client),
                      trello.TrelloClient,
                      "client method did not return a TrelloClient object")
        
    def test_get_board_id(self):
        """Get a board id"""
        board_name = self.config['trello']['test_board']
        board_id = self.config['trello']['test_board_id']
        self.assertEqual(self.trello.get_board_id(board_name),
                         board_id,
                         "Did not get expected board_id")
        
    def test_add_list(self):
        """Add a list to a test board"""
        board = self.trello.get_board(self.config['trello']['test_board'])
        lst = self.trello.add_list(board,self.config['trello']['test_list'])
        self.assertIs(type(lst),
                      trello.List,
                      "Adding a list to test board did not return a List object")
        id = self.trello.get_list_id(board,self.config['trello']['test_list'])
        self.assertEqual(lst.id,
                         id,
                         "Created list id did not match fetched list id")
    
    def test_add_card(self):
        """Add a card to a test list"""
        board = self.trello.get_board(self.config['trello']['test_board'])
        lst = self.trello.get_list(board,self.config['trello']['test_list'])
        card = self.trello.add_card(lst,self.config['trello']['test_card'])
        self.assertIs(type(card),
                      trello.Card,
                      "Adding a card to test list did not return a Card object")
        id = self.trello.get_card_id(lst,self.config['trello']['test_card'])
        self.assertEqual(card.id,
                         id,
                         "Created card id did not match fetched card id")
    
    def test_add_checklist(self):
        """Add a checklist to a test card"""
        board = self.trello.get_board(self.config['trello']['test_board'])
        lst = self.trello.get_list(board,self.config['trello']['test_list'])
        card = self.trello.add_card(lst,self.config['trello']['test_card'])
        import pdb; pdb.set_trace()
        chklst = card.add_checklist("test_checklist",["item1", "item2", "item3"],[True,True,False])
        self.assertIs(type(chklst),
                      trello.Checklist,
                      "Did not get a Checklist object back")
        
        chklst.set_checklist_item('item2',False)

        
    