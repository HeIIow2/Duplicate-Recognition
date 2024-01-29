from typing import Dict, Any
import logging
import time
from enum import Enum
from dataclasses import dataclass, field


def timeit(method):
    logger = logging.getLogger("timeit")

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        logger.info(f"{method.__name__} took {te - ts} seconds:")
        return result

    return timed


class Algorithm(Enum):
    EQUALITY = 1
    PHONETIC_DISTANCE = 2
    URL = 3
    COUNTRY = 4
    VAT_ID = 5
    PHONE = 6


@dataclass
class Comparison:
    entity: Dict[str, Any]
    other_entity: Dict[str, Any]

    field_scores: Dict[str, float] = field(default_factory=dict)
    f_score_sum: float = 0
    count: int = 0

    score: float = 0
