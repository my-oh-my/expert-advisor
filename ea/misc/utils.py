import math


def is_nan(string):
    return string != string


def sign(value):
    return math.copysign(1, value)
