from abc import ABC, abstractmethod
from src.ingestion.models import JobPosting


class SourceAdapter(ABC):
    name: str

    @abstractmethod
    def fetch(self) -> list[JobPosting]:
        ...
