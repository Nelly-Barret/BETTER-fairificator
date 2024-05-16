import matplotlib
import numpy as np
import pandas as pd
from pymongo.command_cursor import CommandCursor

from src.utils.utils import assert_type

matplotlib.rcParams['backend'] = 'TkAgg'  # fix to avoid "Loaded backend macosx version unknown." error
import matplotlib.pyplot as plt
from pymongo.cursor import Cursor

from src.utils.setup_logger import log


class DistributionPlot:
    def __init__(self, cursor: CommandCursor, examination_name: str, y_label: str, vertical_y: bool):
        assert_type(variable=cursor, expected_type=CommandCursor, variable_name="cursor")
        assert_type(variable=examination_name, expected_type=str, variable_name="examination_name")
        assert_type(variable=y_label, expected_type=str, variable_name="y_label")
        assert_type(variable=vertical_y, expected_type=bool, variable_name="vertical_y")

        self.cursor = cursor
        self.examination_name = examination_name
        self.y_label = y_label
        self.vertical_y = vertical_y
        self.__compute_x_and_y_from_cursor("_id", "total")

    def draw(self):
        log.debug(self.x)
        log.debug(self.y)
        # generate colors and assign one of them to each bar
        df = pd.Series(np.random.randint(10, 50, len(self.y)), index=np.arange(1, len(self.y) + 1))
        cmap = plt.cm.tab10
        colors = cmap(np.arange(len(df)) % cmap.N)

        # generate the plot and prettify it
        plt.barh(self.x, self.y, color=colors)
        plt.xlabel("frequency")
        plt.ylabel(self.y_label)
        if self.vertical_y:
            plt.xticks(rotation=90)
        plt.suptitle('Value distribution for Examination ' + self.examination_name)
        plt.suptitle('Value distribution for Examination ' + self.examination_name)
        # print value of each bar
        for index, value in enumerate(self.y):
            plt.text(value, index, str(value))
        plt.show()

    def __compute_x_and_y_from_cursor(self, x_axis: str, y_axis: str):
        log.debug(self.cursor)
        self.x = []
        self.y = []
        for element in self.cursor:
            log.debug(element)
            self.x.append(str(element[x_axis]))
            self.y.append(element[y_axis])
