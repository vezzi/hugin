import trello

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
    
    def get_board(self, name):
        boards = self.client.list_boards()
        for board in boards:
            if board.name == name:
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
        
    def get_card(self, list, name, open=False):
        cards = list.list_cards()
        for card in cards:
            if card.name == name and (not open or not card.closed):
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
     