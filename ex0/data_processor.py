from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[int | str] = []
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
            raise ValueError("Improper numeric data")


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
            raise Exception("Improper text data")


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
    num = NumericProcessor()
    input1: int = 42
    input2: str = "Hello"
    input3: str = "foo"
    input4: list[int] = [1, 2, 3, 4, 5]
    input5: list[str] = ["Hello", "Nexus", "World"]

    print(f" Trying to validate input: '{input1}': {num.validate(input1)}")
    print(f" Trying to validate input: '{input2}': {num.validate(input2)}")

    print(
            f" Test invalid ingestion of string {input3}"
            f" without prior validation:"
            )
    try:
        num.ingest(input3)
    except Exception as e:
        print(f" Got exception: {e}")

    print(f" Processing data: {input4}")
    values: int = 3
    print(f" Extracting {values} values...")

    num.ingest(input4)
    for value in range(0, values):
        print(f" Numeric value {value}: {num.output()[0]}")
    # print(num.output())

    # print("\nTesting Text Processor...")
    # text = TextProcessor()

    # print(f" Trying to validate input: '{input1}': {text.validate(input1)}")
    # print(f" Processing data: {input5}")

    # print(f"Extracting 1 value...")


if __name__ == "__main__":
    main()
