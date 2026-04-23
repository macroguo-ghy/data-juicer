from data_juicer.ops._lazy_imports import load_class, make_class_index

_class_index = make_class_index(__file__)


def __getattr__(name: str):
    value = load_class(__name__, _class_index, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_class_index()))


__all__ = sorted(_class_index())
