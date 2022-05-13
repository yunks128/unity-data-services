from abc import ABC, abstractmethod
from typing import Union


class AuthenticatorAbstract(ABC):
    @abstractmethod
    def authenticate(self, input_auth_cred: dict) -> Union[str, None]:
        return None
