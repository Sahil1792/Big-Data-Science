import time
from mrjob.job import MRJob

class PartBA(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            addr = fields[2]
            value = int(fields[3])

            if value == 0:
                pass

            else:
                yield(addr, value)
        except:
            pass

    def combiner(self, addr, value):
        yield(addr, sum(value))

    def reducer(self, addr, value):
        yield(addr, sum(value))

if __name__ == '__main__':
    #PartTwo.JOBCONF = {'mapreduce.job.reduces': '1'}
    PartBA.run()
