from dataclasses import dataclass, field
from datetime import datetime
from typing import List

@dataclass
class PowerMeasurement:
    timestamp: datetime
    joules_per_second: float
    namespace: str
    joules_total: float

@dataclass
class DataBlock:
    start_time: datetime
    end_time: datetime
    measurements: List[PowerMeasurement]
    measurement_limit: int
    is_complete: bool = False
    sent_to_db: bool = False