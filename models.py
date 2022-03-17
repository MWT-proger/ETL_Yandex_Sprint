from uuid import UUID

from typing import Union, Optional, List, Dict
from pydantic import BaseModel

OBJ_ID = Union[str, str, UUID]
OBJ_NAME = Union[str, str, UUID]


class FilmWorkModel(BaseModel):
    id: Union[int, str, UUID]
    imdb_rating: Optional[float] = None
    genre: Optional[List[Union[str, str, UUID]]] = None
    title: str
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
    writers: Optional[List[Dict[OBJ_ID, OBJ_NAME]]] = None
