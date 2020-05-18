from mrjob.job import MRJob

class PartBB(MRJob):

    def mapper(self, _, line):
        try:
            if len(line.split(',')) == 5:
                fields = line.split(',')
                join_key1 = fields[0]
                join_value = fields[3]
                yield(join_key1, (join_value, 1))

            if len(line.split('\t')) == 2:
                fields = line.split('\t')
                join_key2 = fields[0]
                join_key2 = join_key2[1:-1]
                join_value = int(fields[1])
                yield(join_key2, (join_value, 2))


        except:
            pass

    #def combiner(self, addr, value):
        #yield(addr, sum(value))

    def reducer(self, addr, values):
        block = None
        amt = None


        for value in values:
            if value[1] == 1:
                block = value[0]

            if value[1] == 2:
                amt = value[0]

        if not block is None and not amt is None:
            yield((addr, block), amt)

if __name__ == '__main__':
    #PartBB.JOBCONF = {'mapreduce.job.reduces': '10'}
    PartBB.run()
