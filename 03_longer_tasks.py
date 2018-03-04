import luigi
import os
import time

class FileTarget(luigi.Target):

    def __init__(self, filename):
        self.filename = filename

    def exists(self):
        return os.path.isfile(self.filename)

class HelloTask(luigi.Task):

    def run(self):
        print("thinking about what to say...")
        time.sleep(5)
        with open("hello.txt", "w") as f:
            f.write("Hello Mario!")

    def output(self):
        return FileTarget("hello.txt")

class ReplyTask(luigi.Task):

    def run(self):
        print("thinking about what to say...")
        time.sleep(5)
        with open("reply.txt", "w") as f:
            f.write("Hello Luigi!")

    def output(self):
        return FileTarget("reply.txt")

    def requires(self):
        return HelloTask()

if __name__ == "__main__":
    luigi.build([ReplyTask()])
