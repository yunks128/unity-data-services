from abc import ABC
from copy import deepcopy


class CumulusBase(ABC):
    def __init__(self, cumulus_base: str, cumulus_token: str):
        self.__cumulus_base = cumulus_base[:-1] if cumulus_base.endswith('/') else cumulus_base
        self.__cumulus_token = cumulus_token
        self.__base_headers = {
            'Authorization': f'Bearer {cumulus_token}'
        }
        self._conditions = []

    def with_page_number(self, page_number):
        self._conditions.append(f'page={page_number}')
        return self

    def with_limit(self, limit: int):
        self._conditions.append(f'limit={limit}')
        return self

    def get_base_headers(self):
        return deepcopy(self.__base_headers)

    @property
    def cumulus_base(self):
        return self.__cumulus_base

    @cumulus_base.setter
    def cumulus_base(self, val):
        """
        :param val:
        :return: None
        """
        self.__cumulus_base = val
        return

    @property
    def cumulus_token(self):
        return self.__cumulus_token

    @cumulus_token.setter
    def cumulus_token(self, val):
        """
        :param val:
        :return: None
        """
        self.__cumulus_token = val
        return
