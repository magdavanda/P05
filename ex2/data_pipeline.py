from abc import ABC, abstractmethod
from typing import Any, Protocol
import typing


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """ add processors to the list of processors (register)"""
        if not isinstance(proc, DataProcessor):
            raise TypeError("No processor found")
        else:
            self._processors.append(proc)
        # print(f"Registering {proc.name}")

    def process_stream(self, stream: list[typing. Any]) -> None:
        for data in stream:
            for processor in self._processors:
                if processor.validate(data) is True:
                    processor.ingest(data)
                    break
            else:
                print(
                        f"DataStream error - Can't process element "
                        f"in stream: '{data}'"
                        )

    def print_processors_stats(self) -> None:
        print(" == DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for processor in self._processors:
            print(
                    f"{processor.name}: total "
                    f"{processor.processing_rank} items processed, "
                    f"remaining {len(processor._storage)} on processor"
                    )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            output_storage: list = []
            for value in range(0, nb):
                try:
                    output_storage.append(processor.output())
                except IndexError as e:
                    print(e)


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")

    input: list[Any] = ['Hello world',
                        [3.14, -1, 2.71],
                        [{'log_level': 'WARNING',
                          'log_message': 'Telnet access! Use ssh instead'},
                         {'log_level': 'INFO',
                          'log_message': 'User wil isconnected'}],
                        42, ['Hi', 'five']]

    stream = DataStream()

    stream.print_processors_stats()
    print()

    num = NumericProcessor()
    stream.register_processor(num)
    print(f"Registering {num.name}")
    print()
    print(f"Sending first batch of data on stream: {input}")
    stream.process_stream(input)
    stream.print_processors_stats()
    print()
    print("Registering other data processors")
    print("Send the same batch again")

    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(text)
    stream.register_processor(log)
    stream.process_stream(input)
    stream.print_processors_stats()

    cons_num: int = 3
    cons_txt: int = 2
    cons_log: int = 1

    print(
            f"Consume some elements from the data processors: "
            f"Numeric {cons_num}, Text {cons_txt}, Log {cons_log}"
          )

    for value in range(0, cons_num):
        num.output()

    for value in range(0, cons_txt):
        text.output()

    for value in range(0, cons_log):
        log.output()

    stream.print_processors_stats()

    try:
        print(log.output())
        print(log.output())
    except IndexError:
        print("Empty storage")
        return


if __name__ == "__main__":
    main()
