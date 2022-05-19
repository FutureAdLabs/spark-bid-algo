import time
import sys, os

from components.transformers.score_transformer import Score_T
from components.transformers.parameters.score_parameters import *

from model.score import adlogdata
from model.config import adconfig
from model.bid import admodels

class SparkPipeline:
    ''' spark pipeline helper class'''
    def __init__():
        self.num = 1
    
    def getData(self, file_path, file_type):

        df = self.spark.read.format(file_type)\
        .options(header='true', inferSchema='true')\
        .load(file_path)
        
        return df

    def createSparkConnection(self):
        self.conn = SparkConnection()

        return self.spark, self.sc

    def loadFile(self, df, file_path, file_type, partitions):
        df.write\
        .mode("overwrite")\
        .partitionBy(partitions)\
        .file_type(file_path)

    def score_data(self, df, kpi, model_params):
        # create connection
        spark, sc = self.createSparkConnection()

        # get input data for scoring
        self.df  = self.getData()

        # score data
        score = Score_T(kpi=kpi,
                        modelParams=self.model_params,
                        verbose=2,
                        mutualInformation=False)

        df_scored = df.transform(score.transform)

        # save scored file
        self.loadFile(df_scored)
        
        return df_scored

if __name__ == "__main__":
    c = SparkPipeline()
    c.score_data()
