from abc import ABC, abstractmethod


class UDSAuthorizorAbstract(ABC):
    @abstractmethod
    def authorize(self, username, resource, action) -> bool:
        return False
