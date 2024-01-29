import logging
from dataclasses import dataclass, field
from datetime import time
from typing import Generator, Tuple, Set, List, Callable, Dict, Any
from collections import defaultdict
from enum import Enum





class Dependencies:
    """
    Here are all objects, that are used for dependency injection.
    """
    F_SCORE_FOR_EXACT_MATCH = 10

    ID_COLUMN: str = "id"
    F_SCORES: Dict[str, float] = defaultdict(lambda: 0)
    MATCHING_ALGORITHM: Dict[str, Algorithm] = defaultdict(lambda: Algorithm.EQUALITY)
    THRESHOLDS: Dict[str, float] = defaultdict(lambda: 0)
    NEGATIVE_FIELDS: Set[str] = set()

    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(f"{self.__class__.__name__}Duplicates")

    def get_relevant_entities(self) -> Generator[Dict[str, Any], None, None]:
        return

    def get_refresh_pairs(self) -> Generator[Tuple[int, int], None, None]:
        return

    def get_existing(self) -> Generator[int, None, None]:
        return

    def get_uncompared(self) -> Generator[int, None, None]:
        return

    def write_comparisons(self, comparisons: Generator[Comparison, None, None]):
        return



