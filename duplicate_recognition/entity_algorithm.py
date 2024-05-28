from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cache, lru_cache
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

from .field_algorythm import compare_fields
from .statistics import DASHBOARD
from .utils import Algorithm


class EntityDict(dict):
    # https://stackoverflow.com/a/6229253/16804841
    def __init__(self, entity_generator: Generator[Tuple[int, Dict[str, Any]], None, None], id_column: str, logger: logging.Logger, *args, **kwargs):
        self.entity_generator = entity_generator
        self.id_column = id_column

        self.logger: logging.Logger = logger

        super().__init__(*args, **kwargs)

    def __missing__(self, key: int):
        for entity_id, entity in self.entity_generator:
            self[entity_id] = entity
            if entity_id == key:
                return entity
        
        self.logger.error(f"Entity with id {key} not found.")
        return {
            self.id_column: key
        }


@dataclass
class Comparison:
    """This class is used to store the comparison between two entities. a and b.
    
    Attributes:
        duplicate_recognition: The parent object, coordinating all comparisons.
        entity: a
        other_entity: b
    """

    duplicate_recognition: DuplicateRecognition
    entity: Dict[str, Any]
    other_entity: Dict[str, Any]

    field_scores: Dict[str, float] = field(default_factory=dict)
    f_score_sum: float = 0
    count: int = 0

    score: float = 0

    @property
    @cache
    def pair(self) -> Tuple[int, int]:
        """Gets the ids of the two entities, that are compared.

        Returns:
            Tuple[int, int]: (a.id, b.id)
        """
        _entity = super().__getattribute__("entity")
        _other_entity = super().__getattribute__("other_entity")
        _id_column = super().__getattribute__("duplicate_recognition").ID_COLUMN

        return _entity[_id_column], _other_entity[_id_column]

    def __hash__(self):
        """
        The entities are uniquely identified by their id.
        creating an unique id from 2 integer: https://stackoverflow.com/a/29188068/16804841
        """
        _entity = super().__getattribute__("entity")
        _other_entity = super().__getattribute__("other_entity")
        _id_column = super().__getattribute__("duplicate_recognition").ID_COLUMN

        a, b = _entity[_id_column], _other_entity[_id_column]

        unique_id = a
        unique_id <<= 32
        unique_id += b

        return unique_id

    def _check_for_best(self, entity_id: int):
        """
        If the current comparison has a higher score, than the currently highest score contained in the global best_matches, 
        it will be replaced.

        Args:
            entity_id (int): The id of the entity, that is compared.
        """
        other = self.duplicate_recognition.best_matches[entity_id]

        if other.score <= self.score:
            self.duplicate_recognition.best_matches[entity_id] = self

    def commit(self):
        """
        This function is called after the comparison is finished and everything is calculated.
        It does the following things:
        - checks if the current comparison is the new best comparison for both entities
        """
        a, b = self.pair

        self._check_for_best(a)
        self._check_for_best(b)


class BestMatchDict(dict):
    def __init__(self, *args, duplicate_recognition: DuplicateRecognition,  **kwargs):
        self.duplicate_recognition: DuplicateRecognition = duplicate_recognition

        super().__init__(*args, **kwargs)

    def __missing__(self, key: int):
        return Comparison(duplicate_recognition=self.duplicate_recognition, entity={}, other_entity={}, score=0)


class DuplicateRecognition:
    """
    Here are all objects, that are used for dependency injection.

    TERMINOLOGY:
    - entity: usually a row in the database (e.g. an exhibitor, or a contact)
    - pair: two entity id, that need to be compared
    - comparison: an entity id (entity) and a list of entity ids (entity_pool), that need to be compared with a
    - uncompared: a list of entity ids, that are not compared yet
    - relevant_entities: all the entities, that need to be compared.
    It doesn't matter in which run (if ran with a limit)
    """
    THRESHOLD = 0.7
    F_SCORE_FOR_EXACT_MATCH = 10

    ID_COLUMN: str = "id"
    F_SCORES: Dict[str, float] = defaultdict(lambda: 0)
    MATCHING_ALGORITHM: Dict[str, Algorithm] = defaultdict(lambda: Algorithm.EQUALITY)
    THRESHOLDS: Dict[str, float] = defaultdict(lambda: 0)
    NEGATIVE_FIELDS: Set[str] = set()

    def __init__(self, logger: logging.Logger = None, name: str = None):
        """
        Args:
            logger (logging.Logger, optional):  Defaults to the logger with the name '{name}_duplicates'.
            name (str, optional): Defaults to the class name.
        """
        DASHBOARD.add_statistics(name=name)
        self.kwargs = locals()

        self.best_matches: Dict[int, Comparison] = BestMatchDict(duplicate_recognition=self)

        self.name = name or self.__class__.__name__
        self.logger = logger or logging.getLogger(f"{self.name}_duplicates")

    def get_existing_best_matches(self) -> Generator[Comparison, None, None]:
        """Because the comparisons grow quadratically, it is not possible to always join all comparisons.
        That is why the best closest matching comparison per entity are stored in the database per score.
        This algorithm doesn't work if it only compares a subset of all entities, if the existing best matches are not fetched before the comparison.

        Yields:
            Generator[Comparison, None, None]: _description_
        """
        yield from ()

    def get_relevant_entities(self) -> Generator[Dict[str, Any], None, None]:
        yield from ()

    def get_refresh_pairs(self) -> Generator[Tuple[int, int], None, None]:
        yield from ()

    def get_compared(self) -> Generator[int, None, None]:
        yield from ()

    def get_uncompared(self) -> Generator[int, None, None]:
        yield from ()

    @DASHBOARD.STATISTICS.silent_timeit
    def write_comparisons(self, comparisons: Generator[Comparison, None, None]):
        yield from ()

    def write_best_comparisons(self, comparisons: Generator[Tuple[int, int, Comparison], None, None]):
        yield from ()

    @DASHBOARD.STATISTICS.timeit
    def _map_relevant_entities(self) -> Generator[Tuple[int, Dict[str, Any]], None, None]:
        """
        :return: A dictionary mapping the id of an entity to the entity itself.

        This maps the relevant entities.
        """

        @lru_cache()
        def _clean_value(value: Any) -> Any:
            """
            :param value:
            :return: if it returns None, the entry will be deleted from the entity.
            """
            if isinstance(value, str):
                value = value.strip().lower()

            return value

        def _process_entity(_entity: Dict[str, Any]) -> Dict[str, Any]:
            for key, value in _entity.copy().items():
                if self.F_SCORES[key] <= 0 and key != self.ID_COLUMN:
                    del _entity[key]
                    continue

                _value = _clean_value(value)

                if _value is None or _value == '':
                    del _entity[key]
                    continue

                _entity[key] = _value

            return _entity

        for entity in self.get_relevant_entities():
            yield int(entity[self.ID_COLUMN]), _process_entity(_entity=entity)
        else:
            yield from ()

    @DASHBOARD.STATISTICS.timeit
    def _generate_comparisons(self) -> Generator[Tuple[int, Tuple[int, ...]], None, None]:
        """
        :param self: The dependencies to use
        :return: A generator that yields tuples (a, existing) representing the pairs to compare,
        where an is an entity to be checked and existing is a tuple of existing entities.

        This generates the pairs to compare.

        It first returns all pairs from the edge table, that need to be rechecked.
        Then it returns all pairs from the edge table, that need to be checked for the first time.

        Regarding the second part:
        It works under the assumptions, that if a new entity is scanned, it is scanned with all existing entities.
        1. the id of a new entity is referred to as a
        2. we have a list of all existing entities, which is referred to as existing
        3. all matches are a with existing
        4. after yielding this, a gets added to existing
        """
        # getting all existing pairs that need to be refreshed
        refresh_pairs = self.get_refresh_pairs()
        a, b = next(refresh_pairs, (None, None))

        if a is not None and b is not None:
            _prev_a = a
            existing: Set[int] = {b}

            for a, b in refresh_pairs:
                if a != _prev_a:
                    yield _prev_a, tuple(existing)
                    existing.clear()

                existing.add(b)
                _prev_a = a
            if a != _prev_a:
                yield _prev_a, tuple(existing)
        else:
            self.logger.info("No existing pairs need to be refreshed.")

        # generating all the new pairs
        a = 0
        existing: List[int] = list(self.get_compared())
        uncompared = self.get_uncompared()
        if len(existing) <= 0:
            existing.append(next(uncompared, None))

            # this prevents the program raising an error, if no new pairs exist
            if existing[0] is None:
                logging.info("No new pairs need to be compared.")
                yield from ()
                return

        existing_set = set(existing)
        for a in uncompared:
            if a in existing_set:
                continue

            yield a, tuple(existing)
            existing.append(a)
            existing_set.add(a)

    @DASHBOARD.STATISTICS.compare_wrapper
    def _compare(self, entity: Dict[str, Any], entity_pool: List[Dict[str, Any]]) -> Generator[Comparison, None, None]:
        """Compares one entity with a batch of other entities.
        This approach enables optimizations like skipping fields that are only present in one entity.

        Args:
            entity (Dict[str, Any]): The one entity to compare.
            entity_pool (List[Dict[str, Any]]): All the other entities to compare with.

        Yields:
            Generator[Comparison, None, None]: Yields one comparison for every entity in entity_pool with the one entity.
        """
        best_match: Comparison = Comparison(self, {}, {})
        for other_entity in entity_pool:
            DASHBOARD.STATISTICS.compared_entity_total += 1
            comparison = Comparison(self, entity, other_entity)

            for key in entity.keys():
                if key not in other_entity:
                    continue

                DASHBOARD.STATISTICS.compared_field_total += 1
                a = entity[key]
                b = other_entity[key]

                temp = compare_fields(a, b, self.MATCHING_ALGORITHM[key])
                if temp < self.THRESHOLDS[key]:
                    temp = 0

                if key in self.NEGATIVE_FIELDS:
                    temp = 1 - temp

                comparison.field_scores[key] = temp

                f_score = self.F_SCORES[key]
                temp *= f_score

                comparison.f_score_sum += f_score
                comparison.score += temp
                comparison.count += 1

            comparison.score = comparison.score / comparison.count if comparison.count > 0 else 0

            comparison.commit()
            yield comparison

            if comparison.score > best_match.score:
                best_match = comparison

        self.logger.debug(
            f"Comparing {entity[self.ID_COLUMN]} {'(' + entity.get('firma', '') + ')':<50} "
            f"with {len(entity_pool)} entities. "
            f"{best_match.score:.2f}: {best_match.other_entity.get('firma', '')}"
        )

    def _get_best_comparison_pairs(self) -> Generator[Tuple[int, int, Comparison], None, None]:
        for i, comp in self.best_matches.items():
            _pair = comp.pair
            j = _pair[1] if _pair[0] == i else _pair[0]
            yield i, j, comp

    @DASHBOARD.STATISTICS.timeit
    def execute(self, limit: Optional[int] = None):
        DuplicateRecognition.__init__(**self.kwargs)

        for comparison in self.get_existing_best_matches():
            comparison.commit()

        def _decrement_limit() -> bool:
            nonlocal limit
            limit -= 1
            return limit < 1

        decrement_limit = _decrement_limit if limit is not None else lambda: False
        id_to_entity = EntityDict(self._map_relevant_entities(), self.ID_COLUMN, self.logger)

        for _field in self.NEGATIVE_FIELDS:
            self.F_SCORES[_field] = -1 * self.F_SCORES[_field]

        for a, existing in self._generate_comparisons():
            self.write_comparisons(self._compare(id_to_entity[a], [id_to_entity[b] for b in existing]))

            if decrement_limit():
                break

        self.write_best_comparisons(self._get_best_comparison_pairs())
