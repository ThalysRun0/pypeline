from __future__ import annotations

import inspect, hashlib, os
import functools
from datetime import datetime
from typing import Callable, Dict
from dataclasses import dataclass, field
import copy

from pyspark.sql.dataframe import DataFrame

from statfipy.pipeline import PipeLine, Pipe, PipeMeta, PipeStatus, Piper #, Extracter, Transformer, Loader
from statfipy.logger import Logger, log_timer
logger = Logger("SparkPipeLine")

def extracter(owner: str, 
              dataset: str = "dataset", 
              fails: bool = True,
              name: str | None = None, 
              version: str | None = None, 
              schema: dict | None = None):
    def decorator(func):
        meta = DataPipeMeta(
            owner=owner,
            name=name or func.__name__,
            fails=fails,
            schema=schema,
            kind="extracter",
            version=version,
            sources=[dataset],
            targets=[],
        )
        specs = inspect.get_annotations(func)
        if not "return" in specs:
            raise Exception(f"{func.__name__} must return something\nCheck if function definition is explicit ex: `def func_name(...) -> DataFrame:`")
        if not specs["return"] is DataFrame:
            raise Exception(f"{func.__name__} must return {DataFrame}\nCheck if function definition is explicit \nex: `def func_name(...) -> DataFrame:")
        func.__metadata__ = meta
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                if fails:
                    raise
                return e
        return wrapper
    return decorator

def transformer(owner: str, 
                name: str | None = None, 
                sources: list[str] = ['dataset'], 
                targets: list[str] = ['dataset'], 
                fails: bool = True,
                version: str | None = None, 
                schema: dict | None = None):
    def decorator(func):
        meta = DataPipeMeta(
            owner=owner,
            name=name or func.__name__,
            sources=sources,
            targets=targets,
            fails=fails,
            schema=schema,
            kind="transformer",
            version=version,
        )
        specs = inspect.get_annotations(func)
        if not "return" in specs:
            raise Exception(f"{func.__name__} must return something\nCheck if function definition is explicit ex: `def func_name(...) -> DataFrame:`")
        args = [specs[arg] for arg in specs if arg != "return"]
        if not DataFrame in args:
            raise Exception(f"{func.__name__} must have at least the first argument as type {DataFrame}")
        if not args[0] is DataFrame:
            raise Exception(f"{func.__name__} must its first argument as type {DataFrame}")
        if not (specs["return"] is DataFrame 
                or specs["return"] == list[DataFrame] # prise en charge de plusieurs type de retour
                or specs['return'] == tuple[DataFrame] 
                ):
            raise Exception(f"{func.__name__} must return at least one {DataFrame}\nCheck if function definition is explicit \nex: `def func_name(...) -> DataFrame:`\n`def func_name(...) -> list[DataFrame]:`")
        func.__metadata__ = meta
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                if fails:
                    raise
                return e
        return wrapper
    return decorator

def loader(owner: str, 
           dataset: str = "dataset", 
           fails: bool = True,
           name: str | None = None, 
           version: str | None = None, 
           schema: dict | None = None):
    def decorator(func):
        meta = DataPipeMeta(
            owner=owner,
            name=name or func.__name__,
            sources=[],
            targets=[dataset],
            fails=fails,
            schema=schema,
            kind="loader",
            version=version,
        )
        specs = inspect.get_annotations(func)
        args = [specs[arg] for arg in specs if arg != "return"]
        if not DataFrame in args:
            raise Exception(f"{func.__name__} must have at least one argument of type {DataFrame}")
        func.__metadata__ = meta
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                if fails:
                    raise
                return e
        return wrapper
    return decorator

@dataclass
class DataPipeMeta(PipeMeta):
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
    start: datetime | None = None
    end: datetime | None = None
    sources: list[str] = None
    targets: list[str] = None

    def __data__(self):
        _tmp_datas = ""
        if self.kind == "extracter":
            #datas = f". -> {[x for x in self.sources] if len(self.sources) > 0 else ''}"
            _tmp_datas = f"{[x for x in self.sources] if len(self.sources) > 0 else ''}"
        
        if self.kind == "transformer":
            _tmp_datas = f"{[x for x in self.sources] if len(self.sources) > 0 else ''}"
            _tmp_datas = f"{_tmp_datas} -> {[x for x in self.targets] if len(self.targets) > 0 else ''}"
        
        if self.kind == "loader":
            #_tmp_datas = f"{[x for x in self.targets] if len(self.targets) > 0 else ''} -> ."
            _tmp_datas = f"{[x for x in self.targets] if len(self.targets) > 0 else ''}"
        return _tmp_datas

    def _generate_pipe_id(self, key:str=None):
        _tmp_key = f"{self.name}.{self.__data__()}" if key is None else key
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
                       "data": self.__data__(), 
                       "owner": self.owner, 
                       "version": self.version}])


@dataclass
class DataPipe(Pipe):
    ref: Callable
    metadata: DataPipeMeta
    params: dict = field(default_factory=dict)


@dataclass
class SparkPipeLine(PipeLine):
    data_stack: Dict[str, DataFrame] = field(default_factory=dict)

    def clear(self):
        super().clear()
        self.data_stack = {}
        return self

    @log_timer(logger)
    def run(self) -> None:

        # extracter
        for item in self.pipes:
            pipe: DataPipe = item
            if pipe.metadata.kind == "extracter":
                pipe.metadata._generate_pipe_id()
                pipe.metadata.start = datetime.now()
                logger.info(f"--- {pipe.metadata.kind} {pipe.metadata.name} : {pipe.metadata.pipe_id}")
                # logger.info(f"params: {pipe.params}")
                
                @log_timer(logger)
                def pipe_func():
                    logger.info(f"load : '{pipe.metadata.sources[0]}'")
                    if pipe.metadata.sources[0] in self.data_stack:
                        logger.warning(f"writing on existing DataStack entry '{pipe.metadata.sources[0]}'")
                    self.data_stack[pipe.metadata.sources[0]] = pipe.ref(**pipe.params)
                try:
                    pipe_func()
                    pipe.metadata.status = PipeStatus.SUCCESS
                except Exception as e:
                    pipe.metadata.status = PipeStatus.FAILED
                    pipe.metadata.execution = str(e)
                    if pipe.metadata.fails:
                        pipe.metadata.end = datetime.now()
                        raise e
                    else:
                        pass
                pipe.metadata.end = datetime.now()

        # # transformer
        # for item in self.pipes:
        #     pipe: DataPipe = item
            if pipe.metadata.kind == "transformer":
                pipe.metadata._generate_pipe_id()
                pipe.metadata.start = datetime.now()
                logger.info(f"--- {pipe.metadata.kind} {pipe.metadata.name} : {pipe.metadata.pipe_id}")
                # logger.info(f"params: {pipe.params}")

                @log_timer(logger)
                def pipe_func():
                    _tmp_sources = []
                    # test la disponibilité des sources
                    _actual_source_len = len([x for x in pipe.metadata.sources if x in self.data_stack])
                    if len(pipe.metadata.sources) != _actual_source_len:
                        logger.warning(f"Pipe: '{pipe.metadata.name}' context DataFrame input length is inconsistent (need:{len(pipe.metadata.sources)}, got:{_actual_source_len})")
                    for x in pipe.metadata.sources:
                        logger.info(f"read : '{x}'")
                        if x not in self.data_stack:
                            logger.warning(f"'{x}' not in DataStack -> used None")
                        _tmp_sources.append(self.data_stack[x] if x in self.data_stack else None)

                    _tmp_targets = pipe.ref(*_tmp_sources, **pipe.params)

                    # test la compatibilité de sortie de fonction
                    _out_target = []
                    if isinstance(_tmp_targets, DataFrame):
                        _out_target = [_tmp_targets] # conversion vers une list
                    if isinstance(_tmp_targets, list):
                        _out_target = _tmp_targets
                    if isinstance(_tmp_targets, tuple):
                        _out_target = list(_tmp_targets)
                    _actual_target_len = len(_out_target)
                    if len(pipe.metadata.targets) != _actual_target_len:
                        logger.error(f"Pipe: '{pipe.metadata.name}' context DataFrame output length is inconsistent (need:{len(pipe.metadata.targets)}, got:{_actual_target_len})")

                    _tmp_index = 0
                    for x in pipe.metadata.targets:
                        logger.info(f"load : '{x}'")
                        if x in self.data_stack:
                            logger.warning(f"writing on existing DataStack entry '{x}'")
                        self.data_stack[x] = _out_target[_tmp_index]
                        _tmp_index += 1

                try:
                    pipe_func()
                    pipe.metadata.status = PipeStatus.SUCCESS
                except Exception as e:
                    pipe.metadata.status = PipeStatus.FAILED
                    pipe.metadata.execution = str(e)
                    if pipe.metadata.fails:
                        pipe.metadata.end = datetime.now()
                        raise e
                    else:
                        pass
                pipe.metadata.end = datetime.now()

        # # loader
        # for item in self.pipes:
        #     pipe: DataPipe = item
            if pipe.metadata.kind == "loader":
                pipe.metadata._generate_pipe_id()
                pipe.metadata.start = datetime.now()
                logger.info(f"--- {pipe.metadata.kind} {pipe.metadata.name} : {pipe.metadata.pipe_id}")
                # logger.info(f"params: {pipe.params}")

                @log_timer(logger)
                def pipe_func():
                    # test la disponibilité des sources
                    logger.info(f"read : '{pipe.metadata.targets[0]}'")
                    if not pipe.metadata.targets[0] in self.data_stack:
                        logger.error(f"'{pipe.metadata.targets[0]}' not in DataStack")

                    pipe.ref(self.data_stack[pipe.metadata.targets[0]], **pipe.params)
                try:
                    pipe_func()
                    pipe.metadata.status = PipeStatus.SUCCESS
                except Exception as e:
                    pipe.metadata.status = PipeStatus.FAILED
                    pipe.metadata.execution = str(e)
                    if pipe.metadata.fails:
                        pipe.metadata.end = datetime.now()
                        raise e
                    else:
                        pass
                pipe.metadata.end = datetime.now()

    def extract(self, dataset: str="dataset", fails: bool=True, func: Callable=None, **params) -> SparkPipeLine:
        if func is None: return
        if not isinstance(func, Piper):
            logger.error(f"{func.__name__} n'est pas compatible")
            return
        meta: DataPipeMeta = copy.deepcopy(func.__metadata__)
        if meta.kind != "extracter":
            raise TypeError(f"{meta.name} n'est pas un extracter")

        meta.sources = [dataset]
        meta.fails = fails
        self.pipes.append(Pipe(func, meta, params))
        self._index += 1
        return self

    def transform(self, sources: list[str]=['dataset'], targets: list[str]=['dataset'], fails: bool=True, func: Callable=None, **params) -> SparkPipeLine:
        if func is None: return
        if not isinstance(func, Piper):
            logger.error(f"{func.__name__} n'est pas compatible")
            return
        meta: DataPipeMeta = copy.deepcopy(func.__metadata__)
        if meta.kind != "transformer":
            raise TypeError(f"{meta.name} n'est pas un transformer")

        meta.sources = sources
        meta.targets = targets
        meta.fails = fails
        self.pipes.append(Pipe(func, meta, params))
        self._index += 1
        return self

    def load(self, dataset: str="dataset", fails: bool=True, func: Callable=None, **params) -> SparkPipeLine:
        if func is None: return
        if not isinstance(func, Piper):
            logger.error(f"{func.__name__} n'est pas compatible")
            return
        meta: DataPipeMeta = copy.deepcopy(func.__metadata__)
        if meta.kind != "loader":
            raise TypeError(f"{meta.name} n'est pas un loader")

        meta.targets = [dataset]
        meta.fails = fails
        self.pipes.append(Pipe(func, meta, params))
        self._index += 1
        return self

    def pipe_nodes(self):
        _tmp_out = []
        for i in self.pipes:
            _tmp_out.append(
                {f"{i.metadata.kind} - {i.metadata.name}": 
                 {"params": i.params, 
                  "datas": {"sources": i.metadata.sources,
                            "targets": i.metadata.targets}}})
        return {"name": self.name, "pipes": _tmp_out}

    def pipeline_status(self):
        _out = {"pipeline": self.name, "pipes": []}
        for pipe in self.pipes:
            _out['pipes'].append({"name": pipe.metadata.name, 
                                  "status":pipe.metadata.status.name, 
                                  "duration": (pipe.metadata.end - pipe.metadata.start).total_seconds(),
                                  "execution": pipe.metadata.execution if (pipe.metadata.execution is not None) else ''
            })
        return _out
