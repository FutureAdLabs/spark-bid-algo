import time
import sys, os

from components.transformers.score_transformer import Score_T
from components.transformers.parameters.score_parameters import *

from model.score import adlogdata
from model.config import adconfig
from model.bid import admodels

class SparkPipeline:

    def __init__():
        self.num = 1

    def score_data(self, df, kpi, model_params):
        score = Score_T(kpi=kpi,
                        modelParams=self.model_params,
                        verbose=2,
                        mutualInformation=False)
        df_scored = df.transform(score.transform)
        
        return df_scored