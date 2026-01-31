import hashlib, os
from typing import Protocol, runtime_checkable, Callable, Any
from dataclasses import dataclass, field

@runtime_checkable
class Piper(Protocol):
    def __call__(self, **kwargs) -> Any: ...

# @runtime_checkable
# class Extracter(Protocol):
#     def __call__(self, **kwargs) -> Any: ...

# @runtime_checkable
# class Transformer(Protocol):
#     def __call__(self, **kwargs) -> Any: ...

# @runtime_checkable
# class Loader(Protocol):
#     def __call__(self, **kwargs) -> Any: ...

from enum import Enum

class PipeStatus(Enum):
    WAITING = -1
    FAILED = 1
    SUCCESS = 0

@dataclass
class PipeMeta:
    name: str
    owner: str
    user: str = os.environ['USER']
    fails: bool = True
    status: PipeStatus = PipeStatus.WAITING
    execution: str | None = None
    pipe_id: str | None = None
    kind: str | None = None
    schema: dict | None = None
    version: str | None = None

    def _generate_pipe_id(self, key:str=None):
        _tmp_key = self.name if key is None else key
        m = hashlib.md5()
        m.update(f"{_tmp_key}".encode())
        self.pipe_id = m.hexdigest()

    def __node__(self, _name_force:bool=False):
        _tmp_name = self.pipe_id if self.pipe_id is not None else self.name
        if _name_force:
            _tmp_name = self.name
        return tuple([_tmp_name, 
                      {"kind": self.kind, 
                       "func": self.name,
                       "owner": self.owner, 
                       "version": self.version}])


@dataclass
class Pipe:
    """dataclass Pipe
    Représente un noeud de la pipeline
    """
    ref: Callable
    metadata: PipeMeta
    params: dict = field(default_factory=dict)


@dataclass(init=False)
class PipeLine(Protocol):
    name: str = field(default_factory=str)
    pipes: list[Pipe] = field(default_factory=list[Pipe])
    _index: int = 0

    def __init__(self, name:str=None):
        self.name = name
        self.clear()

    def clear(self):
        self.pipes.clear()
        self._index = 0
        return self

    def run(self) -> None:
        ...

    def iter_metadata(self, _name_force:bool=False):
        for attr in self.__dict__.values():
            if isinstance(attr, Pipe):
                yield attr.metadata.__node__(_name_force)
            elif isinstance(attr, list):
                for p in attr:
                    if isinstance(p, Pipe):
                        yield p.metadata.__node__(_name_force)
    
    def iter_edges(self, _name_force:bool=False):
        nodes = self.metadata_nodes(_name_force)
        for src, dst in zip(nodes, nodes[1:]):
            yield str(src[0]), str(dst[0])

    def metadata_nodes(self, _name_force:bool=False) -> dict[str, PipeMeta]:
        return [x for x in self.iter_metadata(_name_force)]

    def metadata_edges(self, _name_force:bool=False) -> list[tuple[str, str]]:
        return [x for x in self.iter_edges(_name_force)]
    
    def to_dot(self, _name_force:bool=False):
        dot = {}
        nodes = [(name, meta) for name, meta in self.metadata_nodes(_name_force)]
        edges = [(src, dst) for src, dst in self.metadata_edges(_name_force)]
        dot["digraph ETL"] = {"nodes": nodes, "edges": edges}
        return dot
