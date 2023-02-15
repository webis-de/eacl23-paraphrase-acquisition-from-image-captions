import abc
import fileinput
import importlib
import pkgutil


class Parser(metaclass=abc.ABCMeta):
    def __init__(self, file_paths, encoding="utf-8"):
        self.in_file = fileinput.FileInput(file_paths, openhook=fileinput.hook_encoded(encoding))
        self.buffer = []

    @abc.abstractmethod
    def get_next(self):
        pass

    def get_all(self):
        example = self.get_next()
        while example is not None:
            self.buffer.append(example)
            example = self.get_next()

        return self.buffer


def dynamic_load_parsers():
    pkg_path = "paraphrase/parser"
    for _, name, _ in pkgutil.iter_modules([pkg_path]):
        importlib.import_module(pkg_path.replace("/", ".") + "." + name, package=__package__)

    return {cls.NAME: cls for cls in Parser.__subclasses__()}
