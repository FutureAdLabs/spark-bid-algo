import sys, os
from pathlib import Path

path = Path( os.path.join(os.path.dirname(__file__))  )
cpe_path = Path(__file__).parents[2]
sys.path.append( str(cpe_path))

import sys, os
import glob
import json
import funcy as fy
import boto3
import re
import io
import gzip
import logging
import requests
import time
import csv
import hmac
import base64
import math

import numpy as np
import pandas as pd

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

from pprint import pprint
from datetime import datetime , timedelta
from hashlib import sha1
 
from abc import ABC, abstractmethod
from scipy.optimize import basinhopping
from hyperopt import hp, tpe, fmin, STATUS_OK, Trials, SparkTrials
from hyperopt.pyll.base import scope

from json import dumps

# from  utils.score_imports import *

from pyludio.adutils import *
from .config import adconfig

import findspark
findspark.init() 

import pyspark
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import FloatType
from pyspark.sql.functions import lit,col,sum, avg, max, first, min, monotonically_increasing_id, row_number
from pyspark.sql.window import *
import builtins as p


class admodels(adconfig, adlogging):
        
    def __init__(self, config,
                 spark,
                 model_name='FP', 
                 model_path = None,
                 campaign = 'test',
                 adkind = 'web',
                 win_price_name='AdvertiserCurrency',
                 bid_factor = 1,
                 kpi_names = ['ER','CTR','eCTR','VTR','VR','CPA','eCPA','cCPA'],
                 token = None,
                 nmap_func = None,
                 verbose=1,
                 **kwargs):        
        
        self.lcounter = 0
        
        #copy fields from config (adconfig instance to current), 
        #to current instance
        self.__dict__.update(config.__dict__)  
        #
        self.verbose=verbose
        #
        self.token = token
        self.name_map = nmap_func
        self.bigloop = 0
        #
        self.win_price_name = win_price_name
        self.bid_factor = bid_factor        
        #
        self.adkind = adkind            
        self.campaign = campaign
        self.model_name = model_name 
        self.model_path = self.model_filename(model_path)
        self.fjson_list = []
        #
        self.kpi_names = kpi_names
        #
        self.spark = spark
        self.get_model_params(**kwargs)
    
    def get_model_params(self,**kwargs):

        #initialize model parameters
        for x,y in self.config_params.items():
            if not x in kwargs.keys():
                if self.verbose>2:
                    print(f'adding key={x},val={y} to model kwargs from config_params dict')
                kwargs[x] = self.config_params[x]
            else:
                self.config_params[x] = kwargs[x]
                    
        
        model = kwargs['model'] if 'model' in kwargs.keys() else self.model_name        
            
        if model=='FP':
            return self.fp_model_params(**kwargs)
    
    
    def fp_model_params(self,**kwargs):
        
        #if not kwargs:
        #    kwargs={}
        
        # SPEED > 0.5: Explorative Campaign
        self.speed = float(kwargs['speed']) if 'speed' in kwargs.keys() else 0.6
        
        # Choose bidding function
        self.bfid = int(kwargs['bfid']) if 'bfid' in kwargs.keys() else 3
        
        # Cost per 1000 KPI in dollars
        self.basecpmbid = float(kwargs['basecpmbid']) if 'basecpmbid' in kwargs.keys() else 2.0
        self.mincpmbid = float(kwargs['mincpmbid']) if 'mincpmbid' in kwargs.keys() else 2.0          
        self.maxcpmbid = float(kwargs['maxcpmbid']) if 'maxcpmbid' in kwargs.keys() else 7.0
        self.min_bid_factor = self.mincpmbid/self.basecpmbid
        self.max_bid_factor = self.maxcpmbid/self.basecpmbid

        
        #per auction parameters
        self.ncpm = 1000
        self.basebid = float(self.basecpmbid)/float(self.ncpm)
        self.minbid = float(self.mincpmbid)/float(self.ncpm)
        self.maxbid = float(self.maxcpmbid)/float(self.ncpm)
        
        #optimization algorithm
        self.optname = kwargs['optname'] if 'optname' in kwargs.keys() else 'basin'
        
        #bidding function parameter values (should be replaced after training)
        self.bfparams = None
        self.good_model = None
        
    def print_params(self):
        print('------------ model parameters -----------')        
        for x in ['speed','bfid','optname','basecpmbid','mincpmbid','maxcpmbid']:
            value = getattr(self, x)
            print(f'self.{x} = {value}')
        print('------------------------------------------')
        
    def model_params_id(self,**kwargs):
        model_params = dict(speed=self.speed,bfid=self.bfid,
                            maxbid=self.maxcpmbid,minbid=self.mincpmbid,
                            basecpmbid=self.basecpmbid, optname=self.optname,
                            bfparams=', '.join(format(x, "10.3f") for x in self.bfparams),
                            good_model=self.good_model)
        #add whatever is passed
        model_params = dict(model_params, **kwargs)
       
        return {'hashid':make_hash(model_params),'content':pd.Series(model_params)}
    
    def bid_function(self,Score, par):

        wx = 0
        sumw = 0
        T = par[-1] #linear model intercept
        Score = Score.toPandas()
        # print(Score)
        for i, w in enumerate(par[:-1]):
            
            wkpi = self.kpiv[i] #KPI weight
            x = wkpi*Score[self.cnames[i]]
            sumw += w

            #
            if self.bfid in [1,2,3]:
                wx += w*x
            else:
                wx += (1 + x)**w

        if self.bfid==1:  #sigmoid model
            bid =  (1 + np.exp(-(wx/T)**2))**(-1.0) 
        
        elif self.bfid==2: #linear model divided by sum(weight)
            bid =  wx/sumw + T
        
        elif self.bfid==3: #linear model
            bid =  wx + T
            
        else: #exponential model
            bid =  T  * wx
        
        # bid.collect
        # df = spark.createDataFrame()
        # df.withColumn(bid,lit(None))
        return bid*self.basebid
        
    def bid_cpm(self,*args,**kwargs):
        cpkpibid = self.bid_function(*args, **kwargs)
        cpmbid = cpkpibid*self.ncpm
        return cpmbid

    def data_info(self,data,cols=None,srtkey=None):
        if cols is None:
            cols = self.groupby_features()        
        for x in cols:
            print(f'unique values in {x} columns:',len(data[x].unique()))
            
        if not srtkey is None:
            data_sorted = data[data[srtkey]>0].sort_values(srtkey,ascending=False)
            print(f'#data for {srtkey}>0:',len(data_sorted))
            print_full(data_sorted.head())

    def get_XY(self,df):
        try:            
            score_x = [col for col in df.columns if 'score_' in col]
            
            if list(score_x): 
                print('Using score_KPI columns for multi-kpi optimisation ..')
                x = [sub.replace('score_', '') for sub in score_x]
                kpi_names = x
                dfscore = df[score_x]
                dfx = df[x]     
            else:
                print('Using score & target columns for single-kpi modelling .. [score_xyz] columns not found ')
                kpi_names = ['target']             
                dfscore = df.select('score')
                dfx = df.select('target')                       
        except:
            raise

        return dfscore, dfx, kpi_names
    
    def fit(self, data, cpm=1000, info=False, wkpi=None, paypoint=None, **kwargs):
        if info:
            self.data_info(data,srtkey='target')
            
        self.features =data
        self.X, self.Y, self.kpinames = self.get_XY(data)

        #weight to KPIs
        if wkpi is None:
            self.wkpi = {x:1 for x in self.kpinames}   
        else:
            self.wkpi = wkpi

        #sort parameters to in the following order 
        #['ER','CTR','eCTR','VTR','VR']
        self.cnames = []
        self.kpik = []
        self.kpiv = []
        multikpi = False
        for k in self.kpi_names:
            for sn in self.X.columns:
                if 'score_'+k in sn:
                    multikpi = True
                    self.cnames.append(sn)
                    self.kpik.append(k)
                    self.kpiv.append(self.wkpi.get(k,1))
        if not multikpi:
            self.cnames = ['score']
            self.kpik = ['target']
            self.kpiv = [1]
            
        self.xstart = np.random.rand(len(self.kpiv)+1)

        self.paypoint = paypoint
        
        #total cost
        wincol = kwargs.get('price_min',self.win_price_name)
        self.win_price = data.select(wincol) #data[wincol] 

        if self.verbose>2:
            print('self.X.shape, self.win_price.shape',self.X.shape, self.win_price.shape)
        
        #observed cost and number of KPIs 
        costcol = kwargs.get('costcol','price_median')
        CC = self.win_price
        if  costcol in data.columns:
            print(f'Using {costcol} column to calculate observed budget spend ..')
            CC = data.select(costcol)
            
        self.total_budget = CC.groupBy().sum().collect()[0][0]
        sums = [f.sum(x).alias(str(x)) for x in self.Y.columns]
        
        self.nkpi = self.Y.select(sums).collect()[0].asDict().values()      
        self.cpkpi_spent = [float(self.total_budget)/p.max([1.0,n]) for n in self.nkpi]
        return self

    def expected_cpkpi(self,par):
        #
        bid_price = self.bid_function(self.X, par)
        # if pd.isnull(bid_price).any():
        #     print('ERROR: At least 1 bidding price = NaN')
        #     raise
        #
        bid_price = self.spark.createDataFrame(bid_price, FloatType())
        
        # bid_price.show()
        
        df_with_increasing_id = bid_price.withColumn("monotonically_increasing_id", monotonically_increasing_id())
        window = Window.orderBy(col('monotonically_increasing_id'))
        
        bid_price = df_with_increasing_id.withColumn('id', row_number().over(window)).drop('monotonically_increasing_id')
        win_price = self.win_price.withColumn("id", monotonically_increasing_id())
        
        joined = bid_price.join(win_price, bid_price.id == win_price.id,  "right").select(win_price['id'], bid_price['value'],win_price['AdvertiserCurrency'])
        
        # joined.show()
        # print(joined)
        diff = joined.withColumn("Sub", (joined["AdvertiserCurrency"]-joined["value"])).select("Sub") 
        #
        #win_mask = np.where(diff >= 0.0, 1.0, 0.0)
        
        def isawin(x):
            if x>=0: #np.exp(x) > np.random.rand():
                return 1
            else:
                return 0
                    
        #even if model says less money, we could win it  
        # diff.show()
        
        diffArr = [row.Sub for row in diff.select('Sub').collect()]
        mylist = [0 if str(x) == 'None' else x for x in diffArr]
        win_mask = np.array([isawin(x) for x in [row for row in mylist]])
        # print(win_mask)
        
        # Estimate Profit using ML
        expected_budget_spent = np.dot([float(row.value) for row in bid_price.collect()], win_mask)
        
        
        #number of true positives
        sums = [f.sum(x).alias(str(x)) for x in self.Y.columns]
        expected_nkpi = self.Y.select(sums).collect()[0].asDict().values()      
        
        #for each KPI, estimate the cost per 
        expected_cost_per_kpi = [expected_budget_spent/p.max([1.0,n]) for n in expected_nkpi] #~0 is the minimum

        #
        # print(bid_price)
        # print(bid_price.select("value").rdd.max()[0])
        # print(bid_price.select("value").rdd.min()[0])
        # print(expected_budget_spent)
        # print(expected_nkpi)
        
        return {
            'bidprice':bid_price,
            'maxbid':bid_price.select("value").rdd.max()[0],
            'minbid':bid_price.select("value").rdd.min()[0],
            'budget':expected_budget_spent,
            'nkpi':expected_nkpi,
            'cpkpi':expected_cost_per_kpi
        }
        
    def cpkpi_opt_info(self,par):
        #
        d = self.expected_cpkpi(par)
        #
        bid_price = d['bidprice']
        expected_budget_spent = d['budget']
        expected_nkpi = d['nkpi']
        expected_cpkpi = d['cpkpi']
        maxbid = d['maxbid']
        minbid = d['minbid']
        
        
        print('----------------------')
        print('[mindata,minlimit]=',[minbid, self.minbid])
        print('[maxdata,maxlimit]=',[maxbid, self.maxbid])
        print('----------------------')        
        print('********* win_price:')        
        print(self.win_price.limit(10))
        print('********* pred_price:')
        print(bid_price.limit(10))
        print('----------------------')
        print('expected_nkpi=',expected_nkpi)
        print('expected_total_budget_spent/expected_nkpi=',[expected_budget_spent/p.max([1,n]) for n in expected_nkpi])
        print('actual_nkpi=',self.nkpi)
        print('actual_total_budget_spent/kpi',self.cpkpi_spent)
        print('----------------------')
        
    
    def cpkpi_opt(self,par):
        
        print(par)
        #get expected cpkpi params
        d = self.expected_cpkpi(par)
        bid_price = d['bidprice']        
        expected_budget_spent = d['budget']
        expected_nkpi = d['nkpi']
        expected_cpkpi = d['cpkpi']
        maxbid = d['maxbid']
        minbid = d['minbid']
        
        #optimizatin constraint
        opt_range = (minbid>0) and (maxbid <= self.maxbid)
        opt_bid = expected_budget_spent < self.speed * self.total_budget 
        opt_kpi = all([x>0 for x in expected_nkpi])
        opt_constraint = (opt_range) and (opt_bid) and (opt_kpi)

        ll = 0 #~1 is the minimum
        if maxbid < 0:
            #ll += 0.1**(self.ncpm*maxbid)
            ll += -(self.ncpm*maxbid)
        else:
            ll += (self.ncpm*(maxbid-self.maxbid)/2)**2 #punish bid maxbid
        
        if minbid < 0: #if minbid is negative
            #ll += 0.1**(self.ncpm*minbid)
            ll += -(self.ncpm*minbid)/2
        else:
            ll += (self.ncpm*abs(minbid-self.minbid)/2) #punish            
            

        #
        kpi_loss = 0
        for kpi, nkpi,ekpi,cpkpi in zip(self.kpinames, self.nkpi, expected_nkpi, expected_cpkpi):
            kpiterm = nkpi/p.max([1e-2,ekpi]) #~1 is the minimum
            priceterm =  cpkpi  #~0 is the minimum
            lr = self.wkpi.get(kpi, 1)
            if lr>0:
                w = 1.0/lr #weight is inverse learning rate
                kpi_loss += w*kpiterm*priceterm
            
        budget_loss = expected_budget_spent/self.total_budget
        CPKPI_ = kpi_loss * budget_loss + budget_loss + kpi_loss + ll
        #
        if p.sum(expected_nkpi)>0:
            self.lcounter += 1
            dd = [self.ncpm*x for x in [bid_price.select("value").rdd.min()[0],bid_price.select("value").rdd.max()[0],bid_price.agg({'value':'mean'}).collect()[0][0]]]
            if self.verbose>1 and self.lcounter%1500==0:
                print(f'used {self.bfid}th bid func - par={par}, bid={dd}, limit=[{self.minbid}-{self.maxbid}]') 
                print(f"CPKPI = {CPKPI_}, kpi_loss={kpi_loss}, budget_loss={budget_loss}, ll={ll}, maxbid={maxbid}, minbid={minbid}")
        # print(CPKPI_)   
        return CPKPI_
    
    def loss(self, par):
            return par[0]*1000
        
    def train(self,
              opt_algo = 'basin',
              opt_niter = 130,
              opt_xstart = None,
              opt_kwargs = {"method": "L-BFGS-B"},
              disp=True,
              T=0.5,
              stepsize=1,
              **kwargs):

        if opt_xstart is None:
            opt_xstart = self.xstart
            
        #reinit model parameters if 
        self.get_model_params(**kwargs)      

        if self.verbose>0:        
            print('train params:', 'bfid=%s'%self.bfid)
            
        if opt_algo=='Hyperopt':
            opt_algo_fullname = 'Hyperopt'
            print('>>> Running optimization:  %s'%opt_algo_fullname)
            print('>>>  to find bid function parameters by minimizing CPKPI') 
            
            space = {}
            
            for i in range(len(opt_xstart)):
                space[str(i)] = hp.uniform(str(i), 0, 5)

            n_evals = 5

            trials = SparkTrials(parallelism=100)
            ret = fmin(
                      fn=self.cpkpi_opt,
                      space=space,
                      algo=tpe.rand.suggest,
                      max_evals=n_evals,
                      trials=trials
                    )
            if self.verbose>-1:            
                print("----------ret------")
                print(ret)

            x = []
            for i in range(len(opt_xstart)):
                x.append(ret[str(i)])

        
        if opt_algo=='basin':
            opt_algo_fullname = 'Basinhopping'
            print('>>> Running optimization:  %s'%opt_algo_fullname)
            print('>>>  to find bid function parameters by minimizing CPKPI')  
            ret = basinhopping(self.cpkpi_opt, 
                               opt_xstart, 
                               minimizer_kwargs=opt_kwargs, 
                               niter=opt_niter,
                               T=T,
                               stepsize=stepsize,
                               disp=disp,
                               )
            x = ret.x
            
            
        if self.verbose>-1:            
            print("----------ret------")
            print(ret)
        
        if self.verbose>0:        
            _ = self.cpkpi_opt_info(x)
            
        d = self.expected_cpkpi(x)
        #
        CPKPI_ML = d['cpkpi']
        CPKPI_REF = self.cpkpi_spent
        
        self.bfparams = x
        
        self.good_model = any([x < y for x, y in zip(CPKPI_ML, CPKPI_REF)])

        if self.verbose>-1:        
            print('>>> %s' % opt_algo_fullname)
            print('Best Parameters:', self.bfparams)
            print(f'Cost Per 1000 {self.kpinames}:')
            print('With ML $', 1e3*np.array(CPKPI_ML))
            print('No ML $', 1e3*np.array(CPKPI_REF))
            
            self.CPKPI_ML = CPKPI_ML[0]
            self.CPKPI_REF = CPKPI_REF[0]
        
        if(not self.good_model):
            print('WARNING: Optimization did not work properly!')
            print('         1) Try to change bidding function.')
            print('         2) Change inital parameters.')
            print('         3) Cange Temperature.')
            print('         for more info:')
            #help(basinhopping)
            #raise 'notgood model'
            
        return self
    
    def predict(self, X=None,cpm=True):
        if X is None:
            X = self.X

        
        if self.bfparams is None:
            print('No model is found. First train model before using predict')
            raise
            #return pd.Series([np.NaN]*len(X))
    
        else:
            if cpm:
                cpm_price = self.bid_cpm(X, self.bfparams)
                return cpm_price
                
            else:
                bid_price = self.bid_function(X, self.bfparams)            
                return bid_price

    def bidfactor_filter(self,dfbid):
        cond = dfbid >= 1.0  #min filter: bidfactor must be >1
        cond = cond & (dfbid<=self.max_bid_factor) #max filter
        return cond

    def score_to_bidfactor(self,dfscore,**kwargs):
        '''
        Given model is produce, this function transforms
        a score of an impression to a bid value.
        The bidvalues are divided by a basebid to obtain bid_factors        
        '''

        #predict bid price for training/test features
        bid = self.predict(dfscore, cpm=kwargs.get('cpm',True))
        #
        bid_factor = bid/self.basecpmbid        
        bid_factor = bid_factor.round(6)

        #
        nbidline = len(bid_factor[bid_factor>1.0])

        if self.verbose>0:        
            print('-------- bid values (CURRENCY) -------',
                  '\n min bid price',bid.min(),
                  '\n max bix price',bid.max(),
                  '\n -------- bid factors -------',
                  '\n min bid facotr',bid_factor.min(),
                  '\n max bid factor',bid_factor.max(),
                  '\n -------- Number of bidlines -------',
                  '\n #lines with bid_factor >= 1',nbidline,
                  '\n ------------------------------------'
            )
        
        return bid_factor

    def model_filename(self, key='',model_path=None):
        #
        if model_path is None:
            model_path = 'output/%s_%s_model_%s.json'%(self.model_name, 
                                                       self.campaign,
                                                       self.adkind)        
            
        #model dir   
        model_dir = os.path.dirname(model_path)
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
            
        return model_path
    
    def model_to_json(self,BidLinesList, fout, is_blocklist = False,is_targetlist=False, name="Adludio RTB", verbose=0):
        
        nrow = len(BidLinesList)
        if verbose>0:
            print(self.config_params)

        #
        oidvalue = self.config_params.get("OwnerId")
        if is_blocklist:
            bidlistType = 'BlockList'
            resolution_type = 'ApplyMultiplyAdjustment'
            name = "Adludio BlockList RTB"
        elif is_targetlist:
            bidlistType = 'TargetList'
            resolution_type = 'ApplyMultiplyAdjustment'
            name = "Adludio TargetList RTB"
        else:
            bidlistType = "Optimized"
            resolution_type = "ApplyMinimumAdjustment"
        data = {"Name": name,
                "BidListAdjustmentType":bidlistType,                
                "ResolutionType": resolution_type,
                "BidListOwner": self.config_params.get("Owner",""),
                "BidListOwnerId": self.config_params.get(oidvalue,oidvalue),
                "BidLineCount": nrow,                
                "BidLines": BidLinesList
               }    
        #
        with open(fout, 'w') as fp:
            json.dump(data, fp,  indent=4)
    
    def get_proper_data_type(self, bidLines:list) -> dict:
        '''
        bidLine = [{}, {}]
        Check if data types of bidLine value is similar to expected data type
        '''
        # copy bidLine value, not by reference
        bidLines = [key for key in bidLines]
        expected = {
                "AdFormatId": str,
                "Os": str,
                "PlacementPositionRelativeToFold": str,
                "DomainFragment": str,
                "HourOfWeek":int,
                "DeviceType":str,
                "Browser":str,
                "RenderingContext": str,
                "BidAdjustment": float
        }
        result = []
        for bidLine in bidLines:
            for index,key in enumerate(bidLine):
                expected_dtype = expected[key]
                if type(bidLine[key]) is not expected_dtype:
                    print(f"Key => {bidLine[key]}")
                    bidLine[key] = expected_dtype(bidLine[key])
            result.append(bidLine)
        return result


    def write_bidlines_to_file(self,fout, df_out, is_blocklist = False, is_targetlist = False, ndfmax=10000):
        '''
        Write model to JSON or CSV files - locally
        '''
        
        fname, ext = os.path.splitext(fout)
        #
        if ext=='.json':
            ndf = df_out.count()
            if ndf>ndfmax:                
                chunks = df_split(df_out, ndfmax)
                fjson_list = []
                for i,c in enumerate(chunks):
                    BidLines = c.to_dict('records')
                    if i>0:
                        ff = f'{fname}_{i}{ext}'
                    else:
                        ff = f'{fname}{ext}'
                    self.model_to_json(BidLines,ff,verbose=i==0)
                    fjson_list.append(ff)
            else:
                # BidLines = df_out.to_dict('records')
                BidLines = df_out.rdd.map(tuple).collect()
                if is_blocklist:
                    self.model_to_json(BidLines,fout, is_blocklist = True)
                elif is_targetlist:
                    self.model_to_json(BidLines,fout, is_targetlist = True)  
                else:
                    self.model_to_json(BidLines,fout)
    
                fjson_list = [fout]
            #
            self.fjson_list = fjson_list
        else:
            df_out.to_csv(fout, index=False)

        print(f'***********Model Saved to a file: {fout}!*********')


    def write_model_files_to_s3(self,folder,df_out, is_blocklist = False, is_targetlist = False):
        if (is_blocklist == False) and (is_targetlist == False):
            _ = self.s3_df_write(df_out,folder,basename='model')
            _ = self.s3_df_write(self.features,folder,basename='data')
            _ = self.s3_df_write(self.config_params,folder,
                                 basename='params',ext='json')

        #write json to s3
        for ff in self.fjson_list:
            aws_filename = os.path.join(folder, ff)
            bucket_name = self.get_bucket_name(profile_name='adludio')
            arg= 'aws s3 cp %s %s %s'%(ff,
                                       f's3://{bucket_name}/{aws_filename}',
                                       '--profile adludio')
            print('excuting: '+arg)
            subprocess.call(arg, shell=True)


    def save_model(self,features=None,
                   cpm = True,
                   campaign=None,
                   adkind=None,
                   model_path=None,
                   groupby_columns=None,
                   ndfmax = 10000,
                   is_blocklist = False,
                   is_targetlist = False,
                   blocklist_df = None,
                   **kwargs):
    
        #
        bidname = kwargs.get('bidname',self.bidlist_bidname)
        exclude_blocklists =kwargs.get('exclude_blocklists',False)
        
        if not campaign is None:
            self.campaign =campaign

        if not adkind is None:
            self.adkind = adkind
            
        if not model_path is None:
            self.model_path = model_path
        else:
            self.model_path = self.model_filename(model_path)            

        #scored features that we predict bid values
        if features is None:
            df_out = self.features.alias('df_out')
        else:
            df_out = features.alias('df_out')
            

        
        if (is_blocklist == False) and (is_targetlist == False):
            #get bidfactor
            X, Y, kpinames = self.get_XY(df_out)
            bid_factor = self.score_to_bidfactor(X)

            bid_factor_df = self.spark.createDataFrame(bid_factor, FloatType())
            
            df_with_increasing_id = df_out.withColumn("monotonically_increasing_id", monotonically_increasing_id())
            window = Window.orderBy(col('monotonically_increasing_id'))
            df_out = df_with_increasing_id.withColumn('id', row_number().over(window)).drop('monotonically_increasing_id')
            
            # bid_price = bid_price.withColumn("id", monotonically_increasing_id())
            df_with_increasing_id = bid_factor_df.withColumn("monotonically_increasing_id", monotonically_increasing_id())
            window = Window.orderBy(col('monotonically_increasing_id'))
            bid_factor_df = df_with_increasing_id.withColumn('id', row_number().over(window)).drop('monotonically_increasing_id')

            joined = df_out.join(bid_factor_df, df_out.id == bid_factor_df.id,  "right").withColumnRenamed("value", bidname).drop('id')

            #filter bid factor
            cond = self.bidfactor_filter(bid_factor)
            # print(cond)
            # df_out = joined[cond]
            

        #extract/ensure only relevant columns are selected
        columns = self.model_dimensions()
        # df_out = df_out[columns]
        columns =  ",".join(repr(e) for e in columns)
        df_out = joined.select("df_out.AdFormat", "df_out.FoldPosition", "df_out.OS", "df_out.Site", "BidAdjustment")
        df_out.show()

        #filter less performing inventory conbinations
        if not is_blocklist:
            if not blocklist_df is None:
                compare_cols = self.training_features()
                
                df_blocklist = blocklist_df[compare_cols]
                
                opt_index = df_out.set_index(compare_cols).index
                blocl_index = df_blocklist.set_index(compare_cols).index
                if exclude_blocklists:
                    mask = ~opt_index.isin(blocl_index)
                    df_out = df_out.loc[mask]
                else:
                    mask = opt_index.isin(blocl_index)
                    df_out['BidAdjustment'] = np.where(opt_index.isin(blocl_index),0.0, df_out['BidAdjustment'])
            
        # #map names (used in log files) to IDs (used in model)        
        # for k in ['AdFormat', 'FoldPosition', 'Site']: #.columns:
        #     vin = df_out.select(k).collect()[0].asDict().values() #.to_list()
        #     vout = self.name_map.ttd_keyval_map(k,vin)
        #     print(vout)
        #     print(vin)
        #     df_out[k] = vout
            
        
        #transform columns names from log to model format
        # df_out = df_out.rename(columns=self.name_map.ttd_col_map)
        
        #remove duplicate rows
        # cols = [self.name_map.ttd_col_map(x) for x in columns]
        # cols = [self.name_map.ttd_col_map(x) for x in ['AdFormat', 'FoldPosition', 'Site']]
        # _ = cols.remove(bidname)
        #
        # self.data_info(df_out,cols=cols,srtkey=bidname)
        print(df_out.select("BidAdjustment").rdd.max()[0])
        #remove dulicate and sort by values
        df_out = df_out.sort(f.col(bidname).desc())
        df_out = df_out.drop_duplicates()#(subset=cols, keep='first')
        
        if not is_blocklist:
            df_out = df_out.sort(f.col(bidname).desc())
        #
        if self.verbose>0:
            print('df_out[dropby_features] after droping duplicates: ',df_out.count)
            
        if self.verbose>1:        
            print('--------- Here are model output columns -----')
            print(df_out.head())        
            print(df_out.info())
        
        #write model locall
        fout = self.model_path
        if is_blocklist:
            _ = self.write_bidlines_to_file(fout, df_out, ndfmax=ndfmax, is_blocklist = True)
        elif is_targetlist:
            _ = self.write_bidlines_to_file(fout, df_out, ndfmax=ndfmax, is_targetlist = True)
        else:
            _ = self.write_bidlines_to_file(fout, df_out, ndfmax=ndfmax)
        #write csv to s3
        s3folder = kwargs.get('s3folder',None)
        if not s3folder is None:
            if is_blocklist:
                _ = self.write_model_files_to_s3(s3folder, df_out, is_blocklist = True)
            elif is_targetlist:
                _ = self.write_model_files_to_s3(s3folder, df_out, is_targetlist = True)
            else:
                _ = self.write_model_files_to_s3(s3folder, df_out)

        return df_out  