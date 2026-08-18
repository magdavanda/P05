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
                    self._storage.append((self.processing_rank, str(item)))
                    self.processing_rank += 1
            else:
                self._storage.append((self.processing_rank, str(data)))
                self.processing_rank += 1
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
                    self._storage.append((self.processing_rank, item))
                    self.processing_rank += 1
            else:
                self._storage.append((self.processing_rank, data))
                self.processing_rank += 1
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
                    log_string: str = ": ".join(item.values())
                    self._storage.append((self.processing_rank, log_string))
                    self.processing_rank += 1

            elif isinstance(data, dict):
                self._storage.append((self.processing_rank, str(data)))
                self.processing_rank += 1
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
                except IndexError:
                    break    
            plugin.process_output(output_storage)

class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        csv_list: list = []
        for item in data:
            csv_list.append(item[1])
        print(",".join(csv_list))

class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        json_dict: dict = {}
        json_list: list = []
        for item in data:
            json_dict[item[0]] = item[1]
        for key, value in json_dict.items():
            json_list.append(f'"item_{key}": "{value}"')
        print("{", end="")
        print(", ".join(json_list), end="}\n")


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...\n")

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

    print(f"Registering Processors\n")
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    stream.register_processor(num)
    stream.register_processor(text)
    stream.register_processor(log)
    

    print(f"Sending first batch of data on stream: {input}")
    print()
    stream.process_stream(input)
    stream.print_processors_stats()
    print()

    csv_plugin = CSVExportPlugin()
    json_plugin = JSONExportPlugin()

    print()

    data_to_process: int = 3
    print(f"Send {data_to_process} processed data from each processor to a CSV plugin:")
    stream.output_pipeline(data_to_process, csv_plugin)
    print()

    stream.print_processors_stats()
    print()

    input2: list[Any] = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
     [{'log_level': 'ERROR', 'log_message': '500 server crash'},
      {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}],
       [32, 42, 64, 84, 128, 168], 'World hello']

    print(f"Send another batch of data: {input2}")

    print()
    stream.process_stream(input2)
    stream.print_processors_stats()

    print(num._storage)
    print(text._storage)
    print(log._storage)

    data_to_process = 5
    print(f"\nSend {data_to_process} processed data from each processor to a JSON plugin:")
    stream.output_pipeline(data_to_process, json_plugin)
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
