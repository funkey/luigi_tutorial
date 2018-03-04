import luigi
import os
import time

class FileTarget(luigi.Target):

    def __init__(self, filename):
        self.filename = filename

    def exists(self):
        return os.path.isfile(self.filename)

class TrainTask(luigi.Task):

    epoch = luigi.IntParameter()

    def outfile(self):
        return "train_%d.dat"%self.epoch

    def run(self):
        print("training until epoch %d"%self.epoch)
        time.sleep(5)
        open(self.outfile(), "w")

    def output(self):
        return FileTarget(self.outfile())

class InferenceTask(luigi.Task):

    epoch = luigi.IntParameter()

    def outfile(self):
        return "inference_%d.dat"%self.epoch

    def run(self):
        print("predicting from epoch %d"%self.epoch)
        time.sleep(5)
        open(self.outfile(), "w")

    def output(self):
        return FileTarget(self.outfile())

    def requires(self):
        return TrainTask(self.epoch)

class EvaluateTask(luigi.Task):

    epoch = luigi.IntParameter()
    threshold = luigi.IntParameter()

    def outfile(self):
        return "report_%d_%d.dat"%(self.epoch,self.threshold)

    def run(self):
        print("evaluating epoch %d at threshold %d"%(self.epoch,self.threshold))
        time.sleep(5)
        open(self.outfile(), "w")

    def output(self):
        return FileTarget(self.outfile())

    def requires(self):
        return InferenceTask(self.epoch)

class EvaluateAllTask(luigi.Task):

    epoch = luigi.IntParameter()

    def requires(self):
        return [ EvaluateTask(self.epoch, t) for t in range(10) ]

if __name__ == "__main__":
    luigi.build([EvaluateAllTask(200000)], workers=5)
