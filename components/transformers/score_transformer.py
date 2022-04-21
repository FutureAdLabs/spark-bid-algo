import os
import sys

from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from model.config import adconfig
from model.score import adlogdata
from components.transformers.parameters.score_parameters import *

import pyspark.sql.functions as f
from pyspark import keyword_only
from pyspark.ml import Estimator, Model, Transformer

class Score_T(
    Transformer,
    Verbose,
    ModelParams,
    MinScore,
    MinGroupSize,
    Beta,
    KPI,
    RSample,
    KPIAggregation,
    FeatureAggregation,
    CostAggregation,
    BoxPrice,
    Clean,
    ScoringFunction,
    MutualInformation
):
    @keyword_only
    def __init__(
        self,
        verbose=None,
        modelParams=None,
        minScore=None,
        minGroupSize=None,
        beta=None,
        kpi=None,
        rSample=None,
        kpiAggregation=None,
        featureAggregation=None,
        costAggregation=None,
        boxPrice=None,
        clean=None,
        scoringFunction=None,
        mutualInformation=None
    ):
    
        super().__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        
    @keyword_only
    def setParams(
        self,
        verbose=None,
        modelParams=None,
        minScore=None,
        minGroupSize=None,
        beta=None,
        kpi=None,
        rSample=None,
        kpiAggregation=None,
        featureAggregation=None,
        costAggregation=None,
        boxPrice=None,
        clean=None,
        scoringFunction=None,
        mutualInformation=None
    ):
        kwargs = self._input_kwargs
        return self._set(**kwargs)
    
    
    
    
    def set_config(self):
    
            params = self.getModelParams().copy()
            verbose= self.getVerbose()
            
            # Make config param
            print(params, verbose)
            config= adconfig(params=params,verbose=verbose)
            
            return config

    def typeTransform(self, df):
        pivotDF = df.groupBy("type") \
              .sum("KPI") \
              .pivot("type") \
              .sum("sum(KPI)")
        
        pivotDF.printSchema()
        pivotDF.show(truncate=False)
        
        return pivotDF

    def transform(self, dataset):
        
        # Initialise config
        config = self.set_config()
        features = config.feature_names()
        
        # Get all parameters
        parameters=self.extractParamMap()
        parameters={key.name:parameters[key] for key, value in parameters.items()}
        
        adio = adlogdata(config, minScore=parameters['minScore'],minGroupSize=parameters['minGroupSize'])
        dfw = adio.get_FP_weights(df=dataset, **parameters).sort(f.col("score").desc())
                   
    
        return dfw
