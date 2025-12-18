#%%
from __future__ import annotations

from typing import Protocol, runtime_checkable, Callable, Any, Dict, List, Union
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd

# =========================
# 1. Protocols (contrats)
# =========================

@runtime_checkable
class Extracter(Protocol):
    def __call__(self, **kwargs) -> Any: ...


@runtime_checkable
class Transformer(Protocol):
    def __call__(self, data: Any, **kwargs) -> Any: ...


@runtime_checkable
class Loader(Protocol):
    def __call__(self, data: Any, **kwargs) -> Any: ...


# =========================
# 2. Registry / Container
# =========================

@dataclass
class RegistryItem:
    fn: Callable
    metadata: PipeMetadata


Resolvable = Union[str, Callable]

@dataclass
class Container:
    _by_name: dict[str, RegistryItem] = field(default_factory=dict)
    _by_fn: dict[Callable, RegistryItem] = field(default_factory=dict)

    def register(self, metadata: PipeMetadata, fn: Callable) -> None:
        item = RegistryItem(fn=fn, metadata=metadata)

        if metadata.name in self._by_name:
            raise ValueError(f"Duplicate registration: {metadata.name}")

        if fn in self._by_fn:
            raise ValueError(f"Function already registered: {fn}")

        self._by_name[metadata.name] = item
        self._by_fn[fn] = item

    def resolve(
        self,
        ref: Resolvable,
        *,
        kind: str | None = None,
    ) -> RegistryItem:
        if isinstance(ref, str):
            item = self._by_name[ref]
        else:
            item = self._by_fn[ref]

        if kind and item.metadata.kind != kind:
            raise ValueError(
                f"Item '{item.metadata.name}' is not of kind '{kind}'"
            )

        return item

container = Container()


# =========================
# 3. Métadonnées
# =========================

@dataclass
class PipeMetadata:
    name: str
    kind: str | None = None
    owner: str | None = None
    schema: dict | None = None
    version: str | None = None
    current: datetime = datetime.now()
    sla: str | None = None

    def __node__(self):
        return tuple([self.name, {"kind": self.kind, "owner": self.owner, "version": self.version}])


# =========================
# 4. Décorateurs
# =========================

def extracter(name: str | None = None, *, version: str | None = None, owner: str | None = None, sla: str | None = None, schema: dict | None = None):
    def decorator(fn: Extracter) -> Extracter:
        meta = PipeMetadata(
            name=name or fn.__name__,
            owner=owner,
            schema=schema,
            kind="extracter",
            version=version,
            sla=sla,
        )
        fn.__metadata__ = meta
        container.register(meta, fn)
        return fn
    return decorator


def transformer(name: str | None = None, *, version: str | None = None, owner: str | None = None, sla: str | None = None, schema: dict | None = None):
    def decorator(fn: Transformer) -> Transformer:
        meta = PipeMetadata(
            name=name or fn.__name__,
            owner=owner,
            schema=schema,
            kind="transformer",
            version=version,
            sla=sla,
        )
        fn.__metadata__ = meta
        container.register(meta, fn)
        return fn
    return decorator


def loader(name: str | None = None, *, version: str | None = None, owner: str | None = None, sla: str | None = None, schema: dict | None = None):
    def decorator(fn: Loader) -> Loader:
        meta = PipeMetadata(
            name=name or fn.__name__,
            owner=owner,
            schema=schema,
            kind="loader",
            version=version,
            sla=sla,
        )
        fn.__metadata__ = meta
        container.register(meta, fn)
        return fn
    return decorator


# =========================
# 4. Pipe
# =========================

@dataclass
class Pipe:
    ref: Callable
    params: dict = field(default_factory=dict)


@dataclass
class PipeLine(Protocol):
    def run(self) -> None:
        ...

    def iter_metadata(self):
        for attribute_name in  self.__dict__.keys():
            attribute = self.__dict__[attribute_name]
            if isinstance(attribute, Pipe):
                yield container.resolve(attribute.ref).metadata.__node__()
            if isinstance(attribute, List):
                for pipe in attribute:
                    if isinstance(pipe, Pipe):
                        yield container.resolve(pipe.ref).metadata.__node__()
    
    def iter_edges(self):
        nodes = self.metadata_nodes()
        for src, dst in zip(nodes, nodes[1:]):
            yield str(src[0]), str(dst[0])

    def metadata_nodes(self) -> dict[str, PipeMetadata]:
        return [x for x in self.iter_metadata()]

    def metdata_edges(self) -> List[tuple[str, str]]:
        return [x for x in self.iter_edges()]
    
    def to_dot(self):
        dot = {}
        nodes = [(name, meta) for name, meta in self.metadata_nodes()]
        edges = [(src, dst) for src, dst in pipeline.metdata_edges()]
        dot["digraph ETL"] = {"nodes": nodes, "edges": edges}
        return dot


# =========================
# 4. DataPipeLine
# =========================

@dataclass
class DataPipeLine(PipeLine):
    extracter: Pipe | None = None
    transformers: list[Pipe] = field(default_factory=list)
    loader: Pipe | None = None

    def run(self) -> None:
        if not self.loader or not self.extracter:
            raise ValueError("Pipeline incomplet")

        extracter_item = container.resolve(
            self.extracter.ref.__metadata__.name,
            kind="extracter",
        )
        data = extracter_item.fn(**self.extracter.params)

        for step in self.transformers:
            item = container.resolve(
                step.ref.__metadata__.name,
                kind="transformer",
            )
            data = item.fn(data, **step.params)

        loader_item = container.resolve(
            self.loader.ref.__metadata__.name,
            kind="loader",
        )
        loader_item.fn(data, **self.loader.params)

    def extract(self, fn: Callable, **params) -> DataPipeLine:
        meta = fn.__metadata__
        if meta.kind != "extracter":
            raise TypeError(f"{meta.name} n'est pas un extracter")

        self.extracter = Pipe(fn, params)
        return self

    def transform(self, fn: Callable, **params) -> DataPipeLine:
        meta = fn.__metadata__
        if meta.kind != "transformer":
            raise TypeError(f"{meta.name} n'est pas un transformer")

        self.transformers.append(Pipe(fn, params))
        return self

    def load(self, fn: Callable, **params) -> DataPipeLine:
        meta = fn.__metadata__
        if meta.kind != "loader":
            raise TypeError(f"{meta.name} n'est pas un loader")

        self.loader = Pipe(fn, params)
        return self



#%%
@extracter(version="0.1.10", owner="data-team")
def lire_donnees(path: str) -> pd.DataFrame:
    print(f"on lit les données source depuis {path}")
    df = pd.read_csv(path, header=0)
    return df

@transformer(version="0.1.10", owner="viz-team")
def filtre_is_sex_oriented(data: pd.DataFrame, sex: str):
    print(f"on filtre les données transmises par un extracter")
    return data.loc[data.Sex.eq(sex)]

@loader(version="0.1.10", owner="viz-team")
def print_rows(data):
    print(f"on affiche les données")
    print(data)


#%%
# test read function
df = lire_donnees(path="people-100.csv")
df.info()

#%%
# Méthode de construction du pipeline
# injestion de dépendance
pipeline = DataPipeLine(
    extracter=Pipe(lire_donnees, params={"path": "people-100.csv"}),
    transformers=[
        Pipe(filtre_is_sex_oriented, params={"sex": "Female"}),
        Pipe(filtre_is_sex_oriented, params={"sex": "Male"}), # Means result is nothing !
    ],
    loader=Pipe(print_rows),
)

#%%
# Autre méthode en mode SDL
pipeline = DataPipeLine() \
    .extract(lire_donnees, path="people-100.csv") \
    .transform(filtre_is_sex_oriented, **{"sex": "Female"}) \
    .load(print_rows)

#%%
pipeline.run()

#%%
nodes = pipeline.metadata_nodes()
nodes

#%%
edges = pipeline.metdata_edges()
edges

#%%
pipeline.to_dot()

#%%
# --- Build DAG
# This code generates and displays a DAG from a simple ETL pipeline using metadata.
# It assumes networkx and matplotlib are available.

import networkx as nx
import matplotlib.pyplot as plt

# --- Mock metadata and pipeline structure (representative of the registry content)

G = nx.DiGraph()

for name, meta in nodes:
    label = f"{name}\n[{meta['kind']}]\n{meta['owner']} v{meta['version']}"
    G.add_node(name, label=label)

G.add_edges_from(edges)

# %%
plt.figure()
nx.draw(G, with_labels=True)
plt.show()

# %%
