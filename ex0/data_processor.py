from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self.processing_rank: int = 0
        self.name: str = ""

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        current_data = self._storage.pop(0)
        return current_data


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric Processor"

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
               | list[int | float]) -> None:
        if self.validate(data) is True:
            if isinstance(data, list):
                for item in data:
                    self.processing_rank += 1
                    self._storage.append((self.processing_rank, str(item)))
            else:
                self.processing_rank += 1
                self._storage.append((self.processing_rank, str(data)))
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Text Processor"

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

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data) is True:
            if isinstance(data, list):
                for item in data:
                    self.processing_rank += 1
                    self._storage.append((self.processing_rank, item))
            else:
                self.processing_rank += 1
                self._storage.append((self.processing_rank, data))
        else:
            raise ValueError("Improper text data")


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            if all(isinstance(key, str) and isinstance(value, str) for key,
                    value in data.items()):
                return True
            else:
                return False
        elif isinstance(data, list):
            for item in data:
                if not self.validate(item):
                    return False
            return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data) is True:
            if isinstance(data, list):
                for item in data:
                    self.processing_rank += 1
                    log_string: str = ": ".join(item.values())
                    self._storage.append((self.processing_rank, log_string))

            elif isinstance(data, dict):
                self.processing_rank += 1
                self._storage.append((self.processing_rank, str(data)))
        else:
            raise ValueError("Improper log data")


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    input1: int = 42
    input2: str = "Hello"
    input3: str = "foo"
    input4: list[int] = [1, 2, 3, 4, 5]
    input5: list[str] = ["Hello", "Nexus", "World"]
    input6: list[dict[str, str]] = [{"log_level": "NOTICE",
                                    "log_message": "Connection to server"},
                                    {"log_level": "ERROR",
                                    "log_message": "Unauthorized access!!"}]

    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

# ============================
# Numeric Processor Testing
# ============================
    print("Testing Numeric Processor...")
    print(f" Trying to validate input: '{input1}': {num.validate(input1)}")
    print(f" Trying to validate input: '{input2}': {num.validate(input2)}")

    print(
            f" Test invalid ingestion of string '{input3}'"
            f" without prior validation:"
            )
    try:
        num.ingest(input3)
    except Exception as e:
        print(f" Got exception: {e}")

    print(f" Processing data: {input4}")
    num.ingest(input4)
    values: int = 3
    print(f" Extracting {values} values...")

    # print(num._storage)
    for value in range(0, values):
        try:
            print(f" Numeric value {value}: {num.output()[1]}")
        except IndexError:
            print(f" {num.name} Error: Data storage is empty")
            break

# ============================
# Text Processor Testing
# ============================
    print("\nTesting Text Processor...")

    print(f" Trying to validate input: '{input1}': {text.validate(input1)}")
    print(f" Processing data: {input5}")
    text.ingest(input5)

    txt_value = 1
    print(f" Extracting {txt_value} value...")
    # print(text._storage)
    for value in range(0, txt_value):
        try:
            print(f" Text value {value}: {text.output()[1]}")
        except IndexError:
            print(f" {text.name} Error: Data storage is empty")
            break
    # print(text._storage)

# ============================
# Log Processor Testing
# ============================
    print("\nTesting Log Processor...")
    print(f" Trying to validate input '{input2}': {log.validate(input2)} ")
    print(f" Processing data: {input6}")
    log.ingest(input6)
    log_value = 2
    print(f" Extracting {log_value} values...")
    # print(log._storage)
    for value in range(0, log_value):
        try:
            print(f" Log entry {value}: {log.output()[1]}")
        except IndexError:
            print(f" {log.name} Error: Data storage is empty")
            break


if __name__ == "__main__":
    main()
