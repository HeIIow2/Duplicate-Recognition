# Allow direct execution
import json
import os
import sys
import unittest
from typing import Dict, Any, Generator, Tuple
from collections import defaultdict
import random
import logging
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from faker import Faker

from duplicate_recognition import DuplicateRecognition, Algorithm, Comparison


class TestDuplicateRecognition(DuplicateRecognition):
    """
    This generates random Person data and inserts a couple of duplicates.
    """

    ID_COLUMN = "id"
    NEGATIVE_FIELDS = {"country"}
    F_SCORES = defaultdict(lambda: 0, {
        "first_name": 0.3,
        "last_name": 0.5,
        "country": 0.1,
        "vat_id": 1,
        "phone": 1,
        "homepage": 1
    })
    MATCHING_ALGORITHM = defaultdict(lambda: Algorithm.EQUALITY, {
        "first_name": Algorithm.PHONETIC_DISTANCE,
        "last_name": Algorithm.PHONETIC_DISTANCE,
        "country": Algorithm.COUNTRY,
        "vat_id": Algorithm.VAT_ID,
        "phone": Algorithm.PHONE,
        "homepage": Algorithm.URL
    })
    THRESHOLDS = defaultdict(lambda: 0, {
        "first_name": 0.8,
        "last_name": 0.8,
        "country": 1,
        "vat_id": 1,
        "homepage": 0.8
    })

    def __init__(
            self,
            n_compared: int = 100,
            n_uncompared: int = 500,
            error: float = 0.3,
    ):
        self.n_compared = n_compared
        self.n_uncompared = n_uncompared
        self.error = error

        self._auto_increment = 1
        self.relevant_entities = []

        self.compared = self.generate_entities(self.n_compared)
        self.uncompared = self.generate_entities(self.n_uncompared)

        print(*(e for e in self.relevant_entities if "id" not in e))

        super().__init__()

    def generate_entities(self, n: int):
        file = Path("/", "tmp", str(n) + ".json")
        if file.is_file():
            with file.open("r") as f:
                r = json.load(f)
                self.relevant_entities.extend(r)
                self._auto_increment = max(e["id"] for e in r) + 1
                return r

        def _get_entity():
            fake = Faker()
            r = {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "country": fake.country(),
                "vat_id": "DE" + str(random.randint(100000, 999999)),
                "phone": fake.phone_number(),
                "homepage": fake.url()
            }
            # delete 0 - 3 fields
            for j in range(random.randint(0, 5)):
                _key = random.choice(list(r.keys()))
                del r[_key]

            return r

        collection = []

        person = _get_entity()
        for i in range(n):
            person["id"] = self._auto_increment
            collection.append(person)
            self.relevant_entities.append(person)

            if random.random() > self.error:
                person = _get_entity()
            else:
                person = person.copy()

            self._auto_increment += 1

        with file.open("w") as f:
            json.dump(collection, f)
        return collection

    def get_relevant_entities(self) -> Generator[Dict[str, Any], None, None]:
        return self.relevant_entities

    def get_compared(self) -> Generator[int, None, None]:
        for obj in self.compared:
            yield obj["id"]

    def get_uncompared(self) -> Generator[int, None, None]:
        for obj in self.uncompared:
            yield obj["id"]

    def write_comparisons(self, comparisons: Generator[Comparison, None, None]):
        for comp in comparisons:
            pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    TestDuplicateRecognition().execute()
