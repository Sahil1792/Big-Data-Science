import time
from mrjob.job import MRJob

class PartBC(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split()
            addr = fields[0][2:-2]
            block = fields[1][1:-2]
            addr = addr + ' - ' + block
            value = int(fields[2])
            yield(None, (addr, value))

        except:
            pass

    #def combiner(self, addr, value):
        #yield(addr, sum(value))

    def reducer(self, _, values):
        values = sorted(values, reverse=True, key=lambda l: l[1])
        values = values[:10]
        for value in values:
            addr = value[0]
            val = value[1]
            yield(addr, val)

if __name__ == '__main__':
    #PartTwo.JOBCONF = {'mapreduce.job.reduces': '1'}
    PartBC.run()
