from src.profiles.Hospital import Hospital
from src.utils.TableNames import TableNames
from src.utils.constants import NONE_VALUE
import unittest

from utils.Counter import Counter


class TestHospital(unittest.TestCase):
    def test_constructor(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        self.assertIsNotNone(hospital1)
        self.assertIsNotNone(hospital1.identifier)
        self.assertEqual(hospital1.identifier.value, TableNames.HOSPITAL.value + "/123")

        hospital2 = Hospital(id_value=NONE_VALUE, name="MyHospital", counter=counter)
        self.assertIsNotNone(hospital2)
        self.assertIsNotNone(hospital2.identifier)
        self.assertEqual(hospital2.identifier.value, TableNames.HOSPITAL.value + "/1")

    def test_get_type(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        self.assertIsNotNone(hospital1)
        self.assertEqual(hospital1.get_type(), TableNames.HOSPITAL.value)

        hospital2 = Hospital(id_value=NONE_VALUE, name="MyHospital", counter=counter)
        self.assertIsNotNone(hospital2)
        self.assertEqual(hospital2.get_type(), TableNames.HOSPITAL.value)

    def test_to_json(self):
        counter = Counter()
        hospital1 = Hospital(id_value="123", name="MyHospital", counter=counter)
        hospital1_json = hospital1.to_json()

        self.assertIsNotNone(hospital1_json)
        self.assertIn("identifier", hospital1_json)
        self.assertEqual(hospital1_json["identifier"]["value"], TableNames.HOSPITAL.value + "/123")
        self.assertIn("resourceType", hospital1_json)
        self.assertEqual(hospital1_json["resourceType"], TableNames.HOSPITAL.value)
