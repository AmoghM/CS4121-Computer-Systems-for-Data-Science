from __future__ import print_function
import re,sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import SQLContext as sqlcontext
from pyspark.sql.functions import *

class PageRank:
    def __init__(self,path,max_iter=10,init_rank=1.0):
        self.path = path
        self.max_iter = max_iter
        self.init_rank = init_rank

    def fit(self):
        spark = SparkSession.builder.appName("PageRank").getOrCreate()
        data = spark.read.text(self.path).rdd.map(lambda row: row[0])
#         data = spark.read.format('csv').options(delimiter='\t').load(self.path).rdd.map(lambda row:row[0])
        '''
        OUTPUT: data.collect() will retrieve the content inside the RDD.
        ['article1\tarticle2','article1\tarticle4','article2\tarticle3','article3\tarticle1','article4\tarticle2','article5\tarticle6']
        '''
        
        adj_list = data.map(lambda data: self.create_adj_list(data)).distinct().groupByKey().cache()
        
        '''
        OUTPUT: links.collect() will give a <K, Iterables<V> pairs)
        [('article1', <pyspark.resultiterable.ResultIterable at 0x1c6e180d4c8>),
         ('article2', <pyspark.resultiterable.ResultIterable at 0x1c6e180da48>),
         ('article3', <pyspark.resultiterable.ResultIterable at 0x1c6e180d488>),
         ('article4', <pyspark.resultiterable.ResultIterable at 0x1c6e180dd08>),
         ('article5', <pyspark.resultiterable.ResultIterable at 0x1c6e180d0c8>)]
 
        links.mapValues(list).collect() will give the values of the key instead of Iterables
        [('article1', ['article2', 'article4']),
         ('article2', ['article3']),
         ('article3', ['article1']),
         ('article4', ['article2']),
         ('article5', ['article6'])]
        '''
        
        ranks = adj_list.map(lambda key: (key[0], self.init_rank))
        '''
        Initializes the ranks to 1.
        Output: initial_ranks.collect()
        [('article1', 1),
         ('article2', 1),
         ('article3', 1),
         ('article4', 1),
         ('article5', 1)]
        '''
        for iteration in range(self.max_iter):
            mapped_adj_list = self.mapper(adj_list,ranks)
            '''
            OUTPUT: mapped_adj_list.collect()
            [('article2', 0.5), ('article4', 0.5), ('article3', 1.0), ('article1', 1.0), ('article2', 1.0), ('article6', 1.0)]
            '''
            ranks = self.reducer(mapped_adj_list)
            '''
            OUTPUT: ranks.collect()
            [('article2', 1.4249999999999998), ('article3', 1.0), ('article1', 1.0), ('article6', 1.0), ('article4', 0.575)]
            '''
        
        schema = StructType([StructField(str(i), StringType(), True) for i in range(2)])
        df = spark.createDataFrame(ranks, schema)
        top_df = df.orderBy(list(df.columns))
        # top_df = df.sort(asc(df.columns[0]))
        # top_df = top_df.sort(asc(df.columns[1]))
        top_df.limit(5).repartition(1).write.csv("gs://testing-pyspark-123/PageRankOuptut/test2-p1t3.csv", sep='\t')
        spark.stop()
    
    def mapper(self, adj_list, ranks):
        # Generates Key-Value pair where Key is the node and the value is the page rank value of its incoming node
        return adj_list.join(ranks).flatMap(lambda rank: self.getRank(rank[1][0], rank[1][1]))
        
    def reducer(self,mapped_adj_list):
        # Aggregates the output from the mapper along with dampning effect
        return mapped_adj_list.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)    

    def create_adj_list(self,link):
        link = link.encode('utf-8')
        k,v = re.split(r'\t+', link)
        return k,v
    
    def getRank(self,links, rank):
        num_link = len(links)
        for link in links:
            yield (link, rank / num_link)

    def read(self):
        return spark.read.text(self.path).rdd.map(lambda row: row[0])

pg = PageRank('gs://cs_4121/test_set/total_csv_q1/part-00000-930a10b9-a12d-4526-b873-2dd4624714ad-c000.csv',max_iter=10)
pg.fit()

#actual data: gs://cs_4121/test_set/total_csv_q6/part-00000-5aa8823e-4732-434c-824d-5801bf7f9de4-c000.csv
#test data: gs://cs_4121/test_set/total_csv2/part-00000-720fa84e-bf12-46ca-89f9-b732a0b84b22-c000.csv
#small wiki: gs://cs_4121/test_set/total_csv_q1/part-00000-930a10b9-a12d-4526-b873-2dd4624714ad-c000.csv 