from abc import ABC, abstractmethod


class StacTransformerAbstract(ABC):
    @abstractmethod
    def to_stac(self, source: dict) -> dict:
        return

    @abstractmethod
    def from_stac(self, source: dict) -> dict:
        return
