from abc import ABC, abstractmethod


class DownloadGranulesAbstract(ABC):
    @abstractmethod
    def download(self, **kwargs) -> list:
        raise NotImplementedError()
