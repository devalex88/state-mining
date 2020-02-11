#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
An example demonstrating FPGrowth.
Run with:
  bin/spark-submit examples/src/main/python/ml/fpgrowth_example.py
"""
# $example on$
from pyspark.ml.fpm import FPGrowth, PrefixSpan
# $example off$
from pyspark.sql import Row, SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("FPGrowthExample")\
        .getOrCreate()
    sc = spark.sparkContext

    path = "C:\\tmp\\patterns.json"
    df = spark.read.json(path)
    df.printSchema()
    df.show()

    # df = sc.parallelize([Row(sequence=[['a'], ['b'], ['c']]),
    #                     Row(sequence=[['d'], ['e'], ['f']]),
    #                     Row(sequence=[['b'], ['c'], ['d']]),
    #                     Row(sequence=[['b'], ['c'], ['d']])]).toDF()

    prefixSpan = PrefixSpan(minSupport=0.1, maxPatternLength=5, maxLocalProjDBSize=32000000)

    # Find frequent sequential patterns.
    outDf = prefixSpan.findFrequentSequentialPatterns(df).orderBy("freq", ascending=False)
    outDf.show(1000, False)
    pandas_df = outDf.toPandas()
    pandas_df.sort_values('freq', ascending=False ).to_json('C:\\tmp\\freq.json')

    spark.stop()