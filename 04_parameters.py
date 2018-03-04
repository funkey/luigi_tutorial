import luigi
import os
import time

class FileTarget(luigi.Target):

    def __init__(self, filename):
        self.filename = filename

    def exists(self):
        return os.path.isfile(self.filename)

class HelloTask(luigi.Task):

    to_whom = luigi.Parameter()

    def run(self):
        print("thinking about what to say...")
        time.sleep(5)
        with open("hello_%s.txt"%self.to_whom, "w") as f:
            f.write("Hello %s!"%self.to_whom)

    def output(self):
        return FileTarget("hello_%s.txt"%self.to_whom)

class ReplyTask(luigi.Task):

    def run(self):
        print("thinking about what to say...")
        time.sleep(5)
        with open("reply.txt", "w") as f:
            f.write("Hello Luigi!")

    def output(self):
        return FileTarget("reply.txt")

    def requires(self):
        return HelloTask(to_whom="Luigi")

if __name__ == "__main__":
    luigi.build([ReplyTask()])
