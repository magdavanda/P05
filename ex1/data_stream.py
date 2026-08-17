from abc import ABC, abstractmethod
from typing import Any
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self.processing_rank: int = 0

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


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing. Any]) -> None:
        for data in stream:
            for processor in self._processors:
                if processor.validate(data) is True:
                    print("Nice")
                    break
            else:
                print("Not nice")

    def print_processors_stats(self) -> None:
        ...


def main():
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    print(" == DataStream statistics ==")

    input: list[Any] = ['Hello world',
                        [3.14, -1, 2.71],
                        [{'log_level': 'WARNING',
                          'log_message': 'Telnet access! Use ssh instead'},
                         {'log_level': 'INFO',
                          'log_message': 'User wil isconnected'}],
                        42, ['Hi', 'five']]

    stream = DataStream()

    num = NumericProcessor()
    text = TextProcessor()
    # log = LogProcessor()

    stream.register_processor(num)
    stream.register_processor(text)
    # stream.register_processor(log)

    stream.process_stream(input)

# ============================
#
# ============================


if __name__ == "__main__":
    main()
