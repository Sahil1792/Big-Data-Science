import pyspark

def value_trans(line):    # for valid transactions
    try:
        pos = line.split(',')
        if len(pos) != 7:       #checks the length of the position not equal to 7
            return False
        int(pos[3])    # converts to int
        return True
    except:
        return False

def value_cont(line):   # for valid contracts
    try:
        pos = line.split(',')
        if len(pos) != 5:
            return False
        return True
    except:
        return False

sc = pyspark.SparkContext()   # invokes spark

transactions = sc.textFile("/data/ethereum/transactions")     # for aggregating transactions
val_transactions = transactions.filter(value_trans)
map_transactions = val_transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
agg_transactions = map_transactions.reduceByKey(lambda a, b: a+b)

# to get contracts and mapped o/p in memory

contracts = sc.textFile("/data/ethereum/contracts")
val_contracts = contracts.filter(value_cont)
map_contracts = val_contracts.map(lambda l: (l.split(',')[0], None))

joined = agg_transactions.join(map_contracts) # join operations on transactions & contracts

top10 = joined.takeOrdered(10, key = lambda k: -k[1][0])   #sorts to get top 10 transactions

for addr in top10:
    print('{} - {}'.format(addr[0], addr[1][0]))
