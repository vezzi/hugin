import os
import re
import socket

import trello
from hugin.flowcells import Flowcell
from flowcell_status import FlowcellStatus, FC_STATUSES

FC_NAME_RE = r'(\d{6})_([ST-]*\w+\d+)_\d+_([AB]?)([A-Z0-9\-]+)'

class FlowcellMonitor(object):

    def __init__(self, config):
        self._config = config
        # initialize None values for @property functions
        self._trello_board = None
        self._data_folders = None
        self._trello_cards = None
        self._trello_lists = None

    @property
    def config(self):
        return self._config

    @property
    def trello_board(self):
        if not self._trello_board:
            if not self.config.get('trello'):
                raise RuntimeError("'trello' must be in config file")

            config = self.config.get('trello')
            # todo check if board exist

            api_key = config.get('api_key')
            token = config.get('token')
            api_secret = config.get('api_secret')
            client = trello.TrelloClient(api_key=api_key, token=token, api_secret=api_secret)
            board_id = config.get('board_id')
            self._trello_board  = client.get_board(board_id)

        return self._trello_board

    @property
    def data_folders(self):
        if not self._data_folders:
            self._data_folders = self.config.get('data_folders')
            if self._data_folders is None:
                raise RuntimeError("'data_folders' must be in config file")
        return self._data_folders

    @property
    def trello_cards(self):
        if self._trello_cards is None:
            self._trello_cards = self.trello_board.all_cards()
        return self._trello_cards

    @property
    def trello_lists(self):
        if self._trello_lists is None:
            self._trello_lists = self.trello_board.all_lists()
        return self._trello_lists

    def update_trello_board(self):
        for data_folder in self.data_folders:
            self._check_running_flowcells(data_folder)
            self._check_nosync_flowcells(data_folder)
            # move deleted flowcells to the archive list
            self._check_archived_flowcells(data_folder)

    def _check_running_flowcells(self, data_folder):
        # go throw subfolders
        subfolders = filter(os.path.isdir, [os.path.join(data_folder, fc_path) for fc_path in os.listdir(data_folder)])
        for flowcell_path in subfolders:
            # skip non-flowcell folders
            if not re.match(FC_NAME_RE, os.path.basename(flowcell_path)):
                continue

            status = FlowcellStatus(flowcell_path)
            # depending on the type, return instance of related class (hiseq, hiseqx, miseq, etc)
            flowcell = Flowcell.init_flowcell(status)
            if flowcell.check_status():
                # todo: add comment
                # todo: if comment has been added
                pass
            # update flowcell on trello board
            self._update_card(flowcell)

    def _check_nosync_flowcells(self, data_folder):
        # check nosync folder
        nosync_folder = os.path.join(data_folder, 'nosync')
        if os.path.exists(nosync_folder):
            # move flowcell to nosync list
            for nosync_flowcell in os.listdir(nosync_folder):
                flowcell_path = os.path.join(nosync_folder, nosync_flowcell)
                # skip non-flowcell folders
                if not re.match(FC_NAME_RE, os.path.basename(flowcell_path)):
                    continue
                card = self._get_card_by_name(nosync_flowcell)
                # if the card is not on Trello board, create it
                if card is None:
                    status = FlowcellStatus(flowcell_path)
                    flowcell = Flowcell.init_flowcell(status)
                    self._update_card(flowcell)
                else:
                    self._move_card(card, FC_STATUSES['NOSYNC'])

    def _check_archived_flowcells(self, data_folder):
        # if nosync folder exists
        if os.path.exists(os.path.join(data_folder, FC_STATUSES['NOSYNC']).lower()):
            # get cards from the nosync list
            for card in self._get_cards_by_list(FC_STATUSES['NOSYNC']):
                localhost = socket.gethostname()
                # if the flowcell belongs to the server
                if localhost in card.description:
                    # check if the flowcell has been deleted from the nosync folder
                    if card.name not in os.listdir(os.path.join(data_folder, FC_STATUSES['NOSYNC'].lower())):
                        self._move_card(card, FC_STATUSES['ARCHIVED'])

    def _update_card(self, flowcell):
        # todo: beautify the method
        trello_card = self._get_trello_card(flowcell) # None
        flowcell_list = self._get_list_by_name(flowcell.list)

        # if not card on trello board
        if trello_card is None:
            return self._create_card(flowcell)
        else:
            # skip aborted list
            if flowcell.list == FC_STATUSES['ABORTED']:
                return trello_card
            # if card is in the wrong list
            if trello_card.list_id != flowcell_list.id:
                # move card
                trello_card.change_list(flowcell_list.id)

            # if card is in the right list
            else:
                # todo: checkstatus -> taking too long?
                return trello_card

            # update due_time
            trello_card.set_due(flowcell.due_time)
            if flowcell.list == FC_STATUSES['CHECKSTATUS']:
                trello_card.comment(flowcell.status.warning)
            return trello_card

    def _create_card(self, flowcell):
        trello_list = self._get_list_by_name(flowcell.list)
        if not trello_list:
            raise RuntimeError('List {} cannot be found in TrelloBoard {}'.format(flowcell.status, self.trello_board))

        trello_card = trello_list.add_card(name=flowcell.full_name, desc=flowcell.get_formatted_description())
        if flowcell.list == FC_STATUSES['CHECKSTATUS']:
            trello_card.comment(flowcell.status.warning)

    def _get_list_by_name(self, list_name):
        for item in self.trello_lists:
            if item.name == list_name:
                return item
        return None


    def _get_cards_by_list(self, list_name):
        trello_list = self._get_list_by_name(list_name)
        result = []
        for card in self.trello_cards:
            if card.list_id == trello_list.id:
                result.append(card)
        return result

    def _get_card_by_name(self, card_name):
        for card in self.trello_cards:
            if card.name == card_name:
                return card

    def _move_card(self, card, list_name):
        new_list = self._get_list_by_name(list_name)
        new_list.add_card(name=card.name, desc=card.description)
        card.delete()

    def _get_trello_card(self, flowcell):
        for card in self.trello_cards:
            if flowcell.full_name == card.name:
                return card
        return None