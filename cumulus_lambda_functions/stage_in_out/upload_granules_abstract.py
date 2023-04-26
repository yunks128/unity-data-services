from abc import ABC, abstractmethod


class UploadGranulesAbstract(ABC):
    @abstractmethod
    def upload(self, **kwargs) -> list:
        raise NotImplementedError()
