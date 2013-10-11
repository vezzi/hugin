import trello
import datetime

class TrelloUtils(object):
    
    def __init__(self, config):
        self.config = config.get('trello',{})
        self.client = trello.TrelloClient(api_key=self.config.get('api_key',''),
                                          token=self.config.get('token',''),
                                          api_secret=self.config.get('api_secret',''))
    
    def get_board_id(self, name):
        """ Get a board's id
        """
        board = self.get_board(name)
        if board is not None:
            return board.id
        return None
    
    def get_board(self, name, open=False):
        boards = self.client.list_boards()
        for board in boards:
            if board.name == name and (not open or board.closed == False):
                return board
        return None
    
    def get_list(self, board, name, open=False):
        """Get a list from a board"""
        if open:
            lists = board.open_lists()
        else:
            lists = board.all_lists()
        for lst in lists:
            if lst.name == name:
                return lst
        return None
    
    def get_list_id(self, board, name):
        lst = self.get_list(board,name)
        if lst is not None:
            return lst.id
        return None
    
    def add_list(self, board, name):
        lst = self.get_list(board, name, open=True)
        if lst is not None:
            return lst
        return board.add_list(name)
    
    def get_card_on_board(self, board, name):
        """Get a card across the entire board
        """
        for card in board.all_cards():
            if card.name == name:
                card.fetch()
                return card
        return None
        
    def get_card(self, list, name, open=False):
        cards = list.list_cards()
        for card in cards:
            if card.name == name and (not open or not card.closed):
                card.fetch()
                return card
        return None
    
    def get_card_id(self, list, name):
        card = self.get_card(list, name)
        if card is not None:
            return card.id
        return None
    
    def add_card(self, list, name, desc=None):
        card = self.get_card(list,name,open=True)
        if card is not None:
            return card
        return list.add_card(name,desc)

    def change_list(self, card, new_list, skip_list_ids=None, board_id=None):
        """Move a card to a new list if it is not already on the list or if it is not on any
        of the lists in the optional skip_list_id list. Returns True if the card was moved or 
        False otherwise
        """
        
        if card is None:
            return False
        
        if skip_list_ids is None:
            skip_list_ids = []
        
        if board_id is None:
            board_id = card.board_id
            
        # Add or get an object for the new list
        list_obj = self.add_list(card.client.get_board(board_id),new_list)
        # Don't change if the card is already on the new list or if it is on a list in the skip_list_ids
        old_list_id = card.list_id
        if old_list_id == list_obj.id or old_list_id in skip_list_ids:
            return False
        
        # If the board will change, call the change board method
        if board_id != card.board_id:
            card.change_board(board_id,list_id=list_obj.id)
        else:
            card.change_list(list_obj.id)
        card.fetch()
        return True
    
    def sort_cards_on_list(self, list_obj, key=None):
        """Sort the cards on the list using the supplied key function, or alphabetically if this is None
        """
        def _alphabetically(obj):
            return obj.name
        if key is None:
            key = _alphabetically
        for i,card in enumerate(sorted(list_obj.list_cards(), key=key)):
            card._set_remote_attribute('pos',str(i+1))
        
    def sort_lists_on_board(self, board_obj, key=None):
        """Sort the cards on the list using the supplied key function, or alphabetically if this is None
        """
        def _alphabetically(obj):
            return obj.name
        if key is None:
            key = _alphabetically
        import ipdb; ipdb.set_trace()
        for i,lst in enumerate(sorted(board_obj.all_lists(), key=key)):
            lst._set_remote_attribute('pos',str(i+1))
        