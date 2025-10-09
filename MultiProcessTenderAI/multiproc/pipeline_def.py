from collections import OrderedDict

from multiproc.s200.functions_to_run import *
from multiproc.s300.functions_to_run import *

pipeline_def = OrderedDict(
    RAW_TO_INT=[
        my_function_1,
        (my_function_2, ['my_function_3']),   # we need to execute funct_3 before func2
        my_function_3
    ],
    INT_TO_PRM=[
        (my_function_4, ['my_function_3', 'my_function_2']),
        (my_function_5, ['my_function_4']),

    ],
)
