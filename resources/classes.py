# Classes ---------------------------------------------------------------------------------------------------------------------------

"""
Custom error class that catches mismatched timestamps & measurements.
"""
class TimestampError(Exception):
    def __init__(self, message="The number of timestamps does not equal the number of measurements."):
        self.message = message
        super().__init__(self, message)
