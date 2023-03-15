from abc import ABC, abstractmethod


class CatalogGranulesAbstract(ABC):
    @abstractmethod
    def catalog(self, **kwargs):
        raise NotImplementedError()
