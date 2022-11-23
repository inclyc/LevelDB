from abc import ABC, abstractclassmethod

class generator(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractclassmethod
    def generate_write(self, timestamp: int, value: int) -> str:
        pass
    def generate_query(self, left: int, right: int) -> str:
        pass
    def generate_write_head(self) -> str:
        pass
    def generate_query_head(self) -> str:
        pass
