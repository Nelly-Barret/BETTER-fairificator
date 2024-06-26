from datetime import datetime

from analysis.ExecutionAnalysis import ExecutionAnalysis
from config.BetterConfig import BetterConfig
from database.Database import Database
from profiles.Resource import Resource
from utils.Counter import Counter
from utils.TableNames import TableNames
from utils.constants import NONE_VALUE


class Execution(Resource):
    def __init__(self, config: BetterConfig, database: Database, counter: Counter):
        super().__init__(id_value=NONE_VALUE, resource_type=self.get_type(), counter=counter)

        self.created_at = datetime.now().isoformat()
        self.config = config
        self.execution_analysis = ExecutionAnalysis(database)
        self.execution_analysis.run()
        self.database = database
        self.counter = counter

        if self.counter is None:
            self.counter = Counter()
            self.counter.set_with_database(self.database)

    def store_in_database(self):
        self.database.insert_one_tuple(TableNames.EXECUTION.value, self.to_json())

    def get_type(self):
        return TableNames.EXECUTION.value

    def to_json(self):
        return {
            "identifier": self.identifier.to_json(),
            "createdAt": self.created_at,
            "parameters": self.config.to_json(),
            "analysis": self.execution_analysis.to_json()
        }
