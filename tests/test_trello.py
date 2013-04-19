import unittest
import trello
from hugin.trello_utils import TrelloUtils

class TestTrelloUtils(unittest.TestCase):
    
    def setUp(self):
        self.config = {'trello': {'api_key': '35c3947807caa06935842db61619a1c3',
                                  'token': '1846c76339bc24903f71a55ed522d2855fb94c148f2690c4553e50be6ea1baaf',
                                  'api_secret': '64fafa5ca6fa0b190c7322b4166b199d0b734775ac6e8e2b9ca0077b3675dd33',
                                  'test_board': 'test_board',
                                  'test_board_id': '517082eaf79e031b2a001e51',
                                  'test_list': 'test_list',
                                  'test_card': 'test_card'
                                  }
                       }
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
    