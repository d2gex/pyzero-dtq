import abc


class Application(abc.ABC):

    @abc.abstractmethod
    def accept(self, criteria):
        pass

    @abc.abstractmethod
    def run(self, task):
        pass