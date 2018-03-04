import luigi
import os

class FileTarget(luigi.Target):

    def __init__(self, filename):
        self.filename = filename

    def exists(self):
        return os.path.isfile(self.filename)

class HelloTask(luigi.Task):

    def run(self):
        with open("hello.txt", "w") as f:
            f.write("Hello Mario!")

    def outputs(self):
        return FileTarget("hello.txt")

if __name__ == "__main__":
    luigi.build([HelloTask()])
