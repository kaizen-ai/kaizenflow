from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from ucimlrepo import fetch_ucirepo
import numpy as np
import pandas as pd
import findspark

findspark.init()

spark = SparkSession.builder.getOrCreate()

wine = fetch_ucirepo(id=109)
X = wine.data.features
y = wine.data.targets
data = spark.createDataFrame(pd.concat([X, y], axis=1))

assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol='features')
data = assembler.transform(data)

train, test = data.randomSplit([0.7, 0.3])

LR = LinearRegression(featuresCol='features', labelCol='class')
model = LR.fit(train)
eva = model.evaluate(test)
print(eva.rootMeanSquaredError)
print(eva.r2)

pred = model.transform(test)
