import time
import sys, os

from spark.connection import createSparkConnection, closeSparkConnection
from athena_connection import AthenaConnection

from components.transformers.score_transformer import Score_T
from components.transformers.parameters.score_parameters import *

from model.score import adlogdata
from model.config import adconfig
from model.bid import admodels

class SparkPipeline():
    ''' 
    spark pipeline helper class
    '''

    def __init__(self, **kwargs):

        self.filter_by = kwargs.get('filter_by','campaign_id')
        self.filters = kwargs.get('filters','fdbieo5')
        self.start_date = kwargs.get('start_date','2022-01-10')
        self.end_date = kwargs.get('end_date', '2022-01-11')
    
    def getPathFromAthena(self):
        path = AthenaConnection()
        print(f'ATHENA FILE PATH == {path}')

        return path

    def getData(self, file_path=None, file_type=None):
        if file_path is None:
            c = AthenaConnection()
            print(c)
            c.getS3Path()
            print(f'ATHENA FILE PATH == {file_path}')
        if file_type is None:
            file_type = 'csv'

        return file_path

    # def createSparkConnection(self):
    #     spark= SparkConnection()

    #     return spark

    def loadFile(self, df, file_path, file_type, partitions):
        df.write\
        .mode("overwrite")\
        .partitionBy(partitions)\
        .file_type(file_path)

    def score_data(self, *kwargs):

        # get input data for scoring
        # self.getPathFromAthena()
        
        file_path  = self.getData()

        # create connection
        self.spark = createSparkConnection()
        # self.sc = self.spark.sparkContext

        print(f'SPARK SESSION =  {self.spark}')
        # print(F'SPARK CONTEXT = {self.sc}')

        df = self.spark.read.format('csv')\
        .options(header='true', inferSchema='true')\
        .load(file_path)


        # score data
        score = Score_T(kpi='ER',
                        modelParams=self.model_params,
                        verbose=2,
                        mutualInformation=False)

        df_scored = self.df.transform(score.transform)

        # save scored file
        self.loadFile(df_scored)
        score = Score_T.transform(self, self.df)
        
        print(score)

        # print(df_scored)
        
        return df_scored

if __name__ == "__main__":
    c = SparkPipeline()
    c.score_data()
