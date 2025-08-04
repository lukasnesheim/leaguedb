from dataclasses import dataclass

@dataclass
class SleeperMatchup:
    week: int
    sleeper_id_x: str
    score_x: float
    sleeper_id_y: str
    score_y: float