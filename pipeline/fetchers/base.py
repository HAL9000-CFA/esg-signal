from abc import ABC, abstractmethod

from pipeline.models import CompanyProfile


class BaseFetcher(ABC):
    @abstractmethod
    def fetch(self, *args, **kwargs) -> CompanyProfile:
        pass
