import trello
# FOR TESTING PURPOSE

def get_list_by_name(board, list_name):
    all_lists = board.all_lists()
    for item in all_lists:
        print item.name
        if item.name == list_name:
            return item

def get_card_by_name(board, card_name):
    for card in board.all_cards():
        if card.name == card_name:
            return card


def create_card():
    api_key = "35c3947807caa06935842db61619a1c3"
    token = "14f8e61dc122ca436806ad3e52cbd9750665b729d6620d7f3218b253116f7310"
    api_secret = "64fafa5ca6fa0b190c7322b4166b199d0b734775ac6e8e2b9ca0077b3675dd33"
    client = trello.TrelloClient(api_key=api_key,
                                          token=token,
                                          api_secret=api_secret)
    print client
    board_id = "7HA64hbq"
    board = client.get_board(board_id)

    # print board
    # lists = board.all_lists()
    # print type(lists)
    # print dir(lists)
    # print [(item.name, item.id) for item in lists]
    # print dir(board)

    list_name = "Sequencing"
    the_list = get_list_by_name(board, list_name)
    if the_list is None:
        print "the_list is None"
        return
    print the_list.id

    cards = the_list.list_cards()
    print cards

    card_name = "TEST"
    # card = the_list.add_card(card_name)
    # print card.id
    card = get_card_by_name(board, card_name)
    print card

    second_list_name = "Demultiplexing"
    second_list = get_list_by_name(board, second_list_name)
    if second_list is None:
        print "second_list is None"
        return

    card.change_list(second_list.id)

    print second_list.list_cards()


    # the_list = board.get_list(name=the_list)
    # print the_list
    # print board.get_lists(the_list)


if __name__ == "__main__":
    create_card()