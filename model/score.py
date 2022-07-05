import sys, os
import warnings
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from .config import adconfig

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

# Import the beta function and the binomial function
from scipy.stats import beta, binom

import findspark
findspark.init()

# os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

import pandas as pd
import numpy as np

# import pyspark
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.sql.functions import sha2, concat_ws, udf, log
from pyspark.sql.functions import lit,col,sum, avg, max, first, min, mean


class adlogdata(adconfig):

    
    def __init__(self, config,
                 minScore=0,
                 minGroupSize=0):
        
        # Copy fields from config (adconfig instance to current),to current instance
        self.__dict__.update(config.__dict__) 

        self.minscore = minScore
        self.mingroupsize = minGroupSize
        self.verbose = 0
                        
        
    def raw_df_filter(self,df):
        '''
        Filter data after downloading from S3.
        '''
        # Keep only columns defined by self.ml_columns
        if not self.ml_columns is None:
            df_ = df[self.ml_columns]

        # Drop columns with high missing fraction 
        if self.missingfrac>0:
            df_ = df_.dropna(axis=1, thresh=self.missingfrac * (df_.shape[0]))

        df_.dropna(axis=0)

        # Sample the data
        if self.maxrow>0:
            Frac = np.round(self.maxrow / float(df_.shape[0]), 3)

            if(1.0 > Frac):
                df_ = df_.sample(frac=Frac)
                print('    WARNING: Only', Frac * 100, '% of the data are used')
            else:
                print('    All the input data are used to train the Algorithm.')

        return df_

    def add_hash_column(self,**kwargs):
        """
        Add hash column using groupby features.
        """
        df= kwargs.get('df') 
        groupby_columns = kwargs.get("groupby_columns")
        
        # If groupby features not given use instance.
        if groupby_columns is None:
            groupby_columns = self.groupby_features(**kwargs)
            groupby_columns=list(set(df.columns) & set(groupby_columns))
        
        if self.verbose>-1:
                    print("-"*10,f'Hashing features using {groupby_columns}',"-"*10)
                
        df_columns = groupby_columns
        
        df = df.withColumn("hash", sha2(concat_ws("||", * df_columns), 256))
        
        return df        
    
    @staticmethod
    def get_beta_scores(success, trials, rSample=False):
        """
        Estimate the score of an impression.
    
        Args:
            success (int):  Number of times KPI achieved
            trials (int):  Number of time we bought the impression
        """

        a = 1 + success
        b = 1 + trials - success

        mean, var, skew, kurt = beta.stats(a, b, moments='mvsk')
        
        if rSample:
            # Random beta sampling
            score = np.random.beta(a,b)
        else: 
            # Use mean
            score = mean
        
        score = score /np.sqrt(var)
        # score = score/score.max()        

        return score



    def apply_FP_filters(self,df):
        """
        Filter using min score and min groupsize
        """
        if self.verbose>-1:
            pass
            print("-"*10,f'Filtering transformed data set using minscore = {self.minscore} and mingroupsize {self.mingroupsize} ',"-"*10)
            
        if self.minscore>0:
            df = df[df['score']>self.minscore]
            
        if self.mingroupsize>0:
            df = df[df['group_size']>self.mingroupsize]   
            
        return df
            
    
    def score_transform(self,df=None,**kwargs):
        
        dfw = self.get_FP_weights(df=df,**kwargs)
        
        return self.distribute_weights(dfw, df)
  
        
    def compute_kpi_rate(self,dfweighted, KPI):
        """
        Compute rates for KPIs.
        """
        if self.verbose>-1:
            print("-"*10,f'Computing for KPI rates',"-"*10)
            
        if 'ER' in KPI:   
            dfweighted = dfweighted.withColumn("ER", dfweighted['engagement']/(dfweighted['group_size']))
            
        if 'eCTR' in KPI:
            dfweighted = dfweighted.withColumn("eCTR", dfweighted['click']/(dfweighted['engagement']+1))

        if 'CTR' in KPI:
            dfweighted = dfweighted.withColumn("CTR", dfweighted['click']/(dfweighted['group_size']))

        if 'VTR' in KPI: 
            dfweighted = dfweighted.withColumn("VTR", dfweighted['video-end']/(dfweighted['video-start']+1))
            
        if 'VR' in KPI: 
            dfweighted = dfweighted.withColumn("VR", dfweighted['viewable']/(dfweighted['trackable']+1))
            
        if 'cCPA' in KPI: 
            dfweighted = dfweighted.withColumn("cCPA", dfweighted['converted']/(dfweighted['click']+1))
            
        if 'eCPA' in KPI: 
            dfweighted = dfweighted.withColumn("eCPA", dfweighted['converted']/(dfweighted['engagement']+1))
            
        if 'CPA' in KPI: 
            dfweighted = dfweighted.withColumn("CPA", dfweighted['converted']/(dfweighted['group_size']+1))
            
        return dfweighted
                
    def normalize_by_error(self, X,
                           beta,
                           target_col,
                           nomalizer_col,
                           size_infinity):
        """
        Resample from beta distribution or divide ratio by margin of error (moe)
        """

        if self.verbose>0:
            d = 'beta' if beta else 'normal'
            print(f'scaling rate column ={self.target_col} by sampling from {d} distribution')
            
        scaled_rate = []
        irow = 1
        
        Xk = X[target_col].to_numpy()
        
        for percent, trials in zip(Xk, X[nomalizer_col].to_numpy()):
            
            rate = percent/100
            success = int(rate*trials)
            failure = trials - success
            a = 1+success
            b = 1+failure
            
            mean, var, skew, kurt = sp.beta.stats(a, b, moments='mvsk')
            qinterval = sp.beta.interval(0.99,a,b)
            
            # Don't adjust kpi rate if size is large
            if trials > size_infinity:
                scaled_rate.append(rate)
                continue
            
            if beta:
                # Using beta
                sample = np.random.beta(a,b)
                while sample > rate:
                    sample = np.random.beta(a,b)
            else: 
                # Sample from Gaussian
                moe_95 = 1.96
                moe_99 = 2.58
                
                # The 3sigma  left edge 
                mu = qinterval[0]  
                std = np.sqrt(var)
                sample = np.random.normal(mu,std)
                while sample > mean:
                    sample = np.random.normal(mu,std)

            scaled_rate.append(sample)

                
        X[target_col] = scaled_rate
        
        return X

    
    def get_FP_weights(self,df=None,**kwargs):
        
        '''
        This function does groupby by a hash of rows to determine frequency. 
        The groups are then aggregated according to their dtype. 
        We have a few way checking dtype:
           i) df['numeric'].dtype.kind in 'biufc' #biufc: bool,int (signed), unsigned int, float, complex
           ii) using pandas is_string_dtype(df['A']) & is_numeric_dtype(df['B'])
        '''
        # Sanity check
        if df is None:
            warnings.warn("Dataset not passed...........Pass pandas DataFrame using df keyword")
            return 
        
        verbose = kwargs.get('verbose',0)
        modelParams = kwargs.get('modelParams')
        beta = kwargs.get('beta')
        kpi = kwargs.get('kpi')
        scoringFeatures = kwargs.get('scoringFeatures')
        rSample = kwargs.get('rSample')
        kpiAggregation = kwargs.get('kpiAggregation')
        featureAggregation = kwargs.get('featureAggregation')
        costAggregation = kwargs.get('costAggregation')
        boxPrice = kwargs.get('boxPrice')
        clean = kwargs.get('clean')
        scoringFunction = kwargs.get('scoringFunction')
        
        if self.verbose>-1:
            print("-"*10,f'KPIs being used are:',"-"*10)
            print(kwargs)
            
        df_ = df.alias('df_')
        dfnew = self.select_features(df=df_,**kwargs)
        
        print("****"*20)
        print(dfnew)
        
        if not 'hash' in dfnew.columns:            
            dfnew = self.add_hash_column(df=dfnew,**kwargs)
       

        # Add column to store group size
        dfnew = dfnew.withColumn("group_size", lit(1))
        
        cols = dfnew.columns
        
            
        # Aggregate function dict
        aggregation = {}
        
        use_default = True
        if scoringFeatures is not None:
            for x in scoringFeatures:
                if x in featureAggregation.keys():
                    aggregation[x] = featureAggregation[x]
            if len(aggregation) == 0:
                print("*"*10, f'SCORING DIMENTIONS NOT FOUND', "*"*10)
            else:
                use_default = False
                
        if use_default:
            print("*"*10, f'USING DEFAULT DIMENTIONS FOR SCORING', "*"*10)
            for x in dfnew.columns:
                if x in featureAggregation.keys():
                    aggregation[x] = featureAggregation[x]
                
        for x in dfnew.columns:   
            if x in kpiAggregation.keys():
                aggregation[x] = kpiAggregation[x]

            elif x in costAggregation.keys():
                aggregation[x] = costAggregation[x]

                if boxPrice:    
                    def percentile(n):
                        def percentile_(x):
                            return np.percentile(x, n)
                        return percentile_
                    aggregation['price_mean'] = (x, 'mean')
                    aggregation['price_median'] = (x, 'median')
                    aggregation['price_p25'] = (x, percentile(25))
                    aggregation['price_p75'] = (x, percentile(75))
                    aggregation['price_min'] = (x, 'min')
                    aggregation['price_max'] = (x, 'max')
        
        # percentile_25 = f.expr('percentile_approx(PartnerCostInUSDollars, 0.25)')
        # percentile_50 = f.expr('percentile_approx(PartnerCostInUSDollars, 0.5)')
        # percentile_75 = f.expr('percentile_approx(PartnerCostInUSDollars, 0.75)')
        
        # **aggregation
        
        aggregation_new = list(aggregation.values())
        dfweighted = dfnew.groupby('hash').agg(*aggregation_new)#.reset_index()
       
        # Compute to add KPI rate columns
        dfweighted = self.compute_kpi_rate(dfweighted, kpi)
        # dfweighted.show()
        

        if self.verbose>-1:
            print("-"*10,f'Aggregated data shape is',dfweighted.count(),"-"*10)
            
            
        if self.verbose>=0:
            print("-"*10,f'Unique values of feature are:',"-"*10)
            # print(dfweighted.distinct().count())
            print("-"*10)
                
        denom = {'ER':'group_size',
                 'eCTR':'engagement',
                 'CTR':'group_size',
                 'VTR':'video-start',
                 'VR':'trackable',
                 'eCPA':'click',
                 'CPA':'group_size'}
        
        
        @f.pandas_udf("double")
        def get_fp_scores(P: pd.Series, L1: pd.Series)-> pd.Series:
            """
            Estimate the score of an impression.
            Args:
                P (float):  Probability to get an engagement
                L (float):  Number of time we bought the impression
            """
            print("______________________________________")        
            # If L larger than 1e+4 we get overflow
            L = np.where(L1 > 1.e+4, 1.e+4, L1)

            normL = (L / 40.0)
            invL = 1.0/(L + 1)
            #wyabi = 1.0 #/(1.0 - P + invL) #down weight large N_l with no target
            powL = pow(2.1,normL)
            Alp = 2.35 / (powL + 1.0)
            S = P * (L / (L + 13.0)**2)**(Alp) 

            # Add a second term that scales the median of S by the group_size 
            S = S + np.median(S[S>0]) / (L + 1)
            Scor = S / np.max(S)    
            Scor_av = np.std(Scor)
            Scor1 = np.where(L == 1.0, 0.7 * Scor_av, Scor)

            return pd.Series(Scor1)

        for krate in kpi:
            if krate in dfweighted.columns:
                if self.verbose>-1:
                    print("-"*10,f'Scoring {krate}',"-"*10)
                col1 = krate
                col2 = denom[krate]
                if beta:
                    res = scoringFunction(dfweighted[col1].to_numpy(),dfweighted[col2].to_numpy(), rSample=rSample)
                else:       
                    dfweighted = dfweighted.withColumn(f"score_{krate}", get_fp_scores(col1, col2))
                

        if self.verbose>0:
            print("-"*10,f'Using Mean of all scores as final score',"-"*10)
            
        score_cols = [col for col in dfweighted.columns if 'score_' in col]        
   
        # dfweighted = dfweighted.withColumn("score", (col(score_cols[0]) + col(score_cols[1])) /len(score_cols))
         
        if clean:
            cols = [] 
            for c in kpiAggregation.keys():
                if c in dfweighted.columns:
                    cols.append(c)
            #
            dfweighted = dfweighted.drop(labels=cols,axis=1)
  
        
        return self.apply_FP_filters(dfweighted)


    
    @staticmethod
    def distribute_weights(dfw, dff,
                          weight_columns = ['group_size',
                                            'group_prob',
                                            'score'],
                         drophash=True):


        df =  dfw[weight_columns].reindex(dfw['hash']).reset_index()
        dfxx = pd.concat([dff,df],axis=1)
        dfxx.dropna(axis=0,subset=weight_columns, inplace=True)
        if drophash:
            dfxx.drop('hash',axis=1,inplace=True)
        
        return dfxx