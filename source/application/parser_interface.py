from abc import ABC, abstractmethod
from source.core.dto import Task, ParseResult

class BaseParser(ABC):
    @abstractmethod
    async def parse_fast(self, task: Task) -> ParseResult:
        pass

    @abstractmethod
    async def parse_heavy(self, task: Task) -> ParseResult:
        pass

    async def parse(self, task: Task) -> ParseResult:
        if task.mode == "fast":
            return await self.parse_fast(task)
        return await self.parse_heavy(task)