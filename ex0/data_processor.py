from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[int, str] = []
        self.processing_rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        return tuple(self._storage)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, int | float):
            return True
        elif isinstance(data, list):
            if all(isinstance(item, int | float) for item in data):
                return True
            else:
                return False
        else:
            return False

    def ingest(self, data: int | float | list[int] | list[float]
               | list[int | float]):
        if self.validate(data) is True:
            self._storage.append(data)
            self.processing_rank += 1
        else:
            raise Exception("Invalid input")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            if all(isinstance(item, str) for item in data):
                return True
            else:
                return False
        else:
            return False

    def ingest(self, data: str | list[str]):
        if data.validate() is True:
            if isinstance(data, list):
                for item in data:
                    self._storage.append(data)
                    self.processing_rank += 1
            else:
                self._storage.append(data)
                self.processing_rank += 1
        else:
            raise Exception("Invalid input")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict[str, str] | list[str]):
            return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[str]):
        self._storage.append(data)


def main():
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    abc = NumericProcessor()
    input1 = 42
    input2 = "Hello"
    answer1 = abc.validate(input1)
    answer2 = abc.ingest(input2)
    # answer2 = abc.validate(input2)

    abc.ingest(input1)
    print(f" Trying to validate input: '{input1}': {answer1}")
    print(f" Trying to validate input: '{input2}': {answer2}")
    print(abc._storage)
    print(abc.processing_rank)


if __name__ == "__main__":
    main()
