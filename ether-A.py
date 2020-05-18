import time
from mrjob.job import MRJob

class PartA(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        try:
            t = time.gmtime(int(fields[6]))
            year = t.tm_year
            month = t.tm_mon
            yield((year, month), 1)
        except:
            pass

    def combiner(self, tm, count):
        yield(tm, sum(count))

    def reducer(self, tm, count):
        yield(tm, sum(count))

if __name__ == '__main__':
    PartA.JOBCONF = {'mapreduce.job.reduces': '1'}
    PartA.run()
