from pydantic import BaseModel


class Village(BaseModel):
    id: int
    name: str
    x: int
    y: int
    player_id: int
    points: int
    rank: int


class Player(BaseModel):
    id: int
    name: str
    tribe_id: int
    villages: int
    points: int
    rank: int
    att_bash: int
    att_rank: int
    def_bash: int
    def_rank: int
    sup_bash: int
    sup_rank: int
    all_bash: int
    all_rank: int


class Tribe(BaseModel):
    id: int
    name: str
    tag: str
    member: int
    villages: int
    points: int
    all_points: int
    rank: int
    att_bash: int
    att_rank: int
    def_bash: int
    def_rank: int
    all_bash: int
    all_rank: int
    sup_bash: int
    sup_rank: int
