import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import json
import warnings
from datetime import datetime
import numpy as np
# from utils.mlutils import *
from pyludio.adutils import *
from pyspark.sql.functions import lit

class adconfig():
    def __init__(self,
                 params=None, #'params/settings_ttd.json',  
                 startdate = None,
                 enddate = None,
                 maxrow=0,
                 missingfrac=0.8,
                 mlmodel='FP',
                 speedtemp = 2,                 
                 overwrite=False,
                 verbose=1,
                 var=2,
                 #
                 rootdir = None,
                 profile_name = 'adludio',
                 bucket_name = None,
                 session=None,
                 localdata=False,
                 **kwargs
                ):
        
        #
        self.verbose=verbose
        self.overwrite=overwrite
        
        #initialize config params
        
        if params: #read config params from json file
            self.config_params = self.read_json_params(params)
        else:
            self.config_params = self.make_params(startdate, enddate, 
                                                 lineitemids = lineitemids,
                                                 speedtemp = speedtemp)    
        _ = self.set_days()
        
        
        self.kpi_rate_to_name = {k:v for k,v in zip(['ER','CTR','eCTR','VTR','VR','cCPA','eCPA','CPA'],
                             ['engagement','click','video-end','video-start',
                             'viewable','trackable','converted'])}
        self.kpi_name_to_rate = {v:k for k,v in zip(['ER','CTR','eCTR','VTR','VR','cCPA','eCPA','CPA'],
                             ['engagement','click','video-end','video-start',
                             'viewable','trackable','converted'])}  
        
        #filtering and other important parameters
        self.kwargs = kwargs
        self.mlmodel = mlmodel
        self.missingfrac = missingfrac
        self.maxrow = maxrow
        self.bidlist_bidname = self._bidlist_bidname()
                        
    def make_params(self,startdate, enddate,
                    lineitemids=None,speedtemp=2,
                    biddict={}):    
        
        '''Make config parameters from passed variables'''
        

        params['lineitems'] = lineitemids

        #get date ranges
        params['day1'], params['day2'] = [startdate, enddate]

        #get optimization agressivness parameter
        params['speed'] = self.get_opspeed(str(int(speedtemp)) )

        
        d = {'basecpmbid':2,'mincpmbid':2,'maxcpmbid':6}
        for x in ['basecpmbid','mincpmbid','maxcpmbid']:
            params[x] = biddict.get(x,d[x]) 
        
        #            
        return params

    def get_days(self):
        return days_between(self.config_params['day1'], self.config_params['day2'])
        
    def set_days(self):
        days = self.get_days()
        self.config_params['days'] = days
        
        return days
    
    def read_json_params(self,obj):    
        '''Read parameters from json file'''

        params = {}

        if isinstance(obj,str):
            if self.verbose>0:
                print(f'reading setting from: {path}')
            #
            with open(path) as f:
                data = json.load(f)
                
        elif isinstance(obj,dict):
            data = obj
            
        else:
            print('Unknown obj for read_json_params: ',type(obj))
            print(obj)
            raise

        #"CamapignName"
        cols = data.pop("ColumnFilters")
        params["ColumnFilters"] = {item:data[item] for item in cols}
        
        #get date rages
        d1 = data.pop("date_start")
        d2 = data.pop("date_end")
        #
        day1a = datetime.strptime(d1, '%Y-%m-%d')
        day2a = datetime.strptime(d2, '%Y-%m-%d')
        #
        params['day1'] = day1a.date()
        params['day2'] = day2a.date()
        
        #get other params
        speedtemp = data.pop("speed")
        params['speed'] = self.get_opspeed(speedtemp)

        params = dict(**params, **data)
        #for x in ['basecpmbid','mincpmbid','maxcpmbid']:
        #    params[x] = data[x]
        #
            
        return params
    
    @staticmethod
    def get_opspeed(speedtemp):
        '''get optimization agressivness parameter'''
        s = 1.0
        if speedtemp in [str(i) for i in range(1,9)]: 
             s = {str(i+1):round(0.15+i*0.1,2) for i in range(9)}[speedtemp]
        return s


    def print_params(self,params=None,days=False):
        if params is None:
            params = self.config_params
        for k, v in params.items():
            if k!='days' or days:
                print('%s = '%k,v)
        
        
    def feature_dtypes(self,columns,fillval=False):
        '''
        The data types of all the possible columns in the log files.
        The columns from TTD impression, click, and conversion logs.
        '''
        if self.verbose>-1:
            print("-"*10,f'Generating data type casting dictionary',"-"*10)
        
        # Time data type features
        time_cols = ['LogEntryTime','ProcessedTime']
        
        # Integer data type features
        int_cols = ['OS','Frequency','UserHourOfWeek',
                    'DeviceType','OSFamily','Browser',
                    'CarrierID','TemperatureBucketStartInCelsiusName',
                    'TemperatureBucketEndInCelsiusName',
                    'AdsTxtSellerTypeId']
        
        # Integer Target  features
        label_cols = ['target','engagement','click','converted','video-start', 'video-end','trackable', 'viewable', 'impression']
        
        # Decimal data type features
        float_cols = ['PartnerCurrencyExchangeRateFromUSD',
                      'AdvertiserCurrencyExchangeRateFromUSD',
                      'MediaCostInBucks','FeeFeatureCost',
                      'DataUsageTotalCost','TTDCostInUSD',
                      'PartnerCostInUSD','AdvertiserCurrency','AdvertiserCostInUSD',
                      'Latitude','Longitude','TemperatureInCelsiusName', 'Cost']
       

        d = {}
        fval = {}
        for c in columns:
            if c in int_cols or c in label_cols or 'target:' in c:
                d[c] = int
                fval[c] = 0
            elif c in float_cols:
                d[c] = float
                fval[c] = 0
            else:
                d[c] = str
                fval[c] = ''
                
        if self.verbose>0:
            print("-"*10,f'Casting dictionary to be used',"-"*10)
            print(d)
            
        if fillval:
            return d, fval
        else:
            return d
        
    
        
        
    def training_dimensions(self,**kwargs):
        '''
        The column names of the features without labels.
        '''
        if self.verbose>-1:
            print("-"*10,f'Getting training dimensions',"-"*10)
                
        # model = kwargs.get('model',self.mlmodel)
        df = kwargs.get('df',None)
        model = 'FP'
        use_mutualinf = kwargs.get('mutualInformation',False)
        scoring_features = kwargs.get('scoringFeatures', None)
        
        base_features=['Country', 'AdvertiserId', 'CampaignId','AdgroupId', 'AdFormat', 
                       "FoldPosition",'RenderingContext', 'OS','DeviceType', 'Browser',
                       'Site']
        ColNames = []
        # if for_aggregation:
        print(F'SCORING FEATURES = {scoring_features}')
        if scoring_features is not None:
            if self.verbose>-1:
                print("-"*10,f'Using custom features are',"-"*10)
                print(scoring_features)
            for dim in scoring_features:
                if not dim in df.columns:
                    print('*'*40)
                    print(f'FEATURE {dim} NOT IN {df.columns}')
                else:    
                    ColNames.append(dim)

        
        # Using mutual information for the best training features
        elif use_mutualinf:
            if self.verbose>-1:
                print("-"*10,f'Using mutual information to generate best features',"-"*10)
                
            ColNames=['AdFormat', 'Frequency', 'Site', 
                      'FoldPosition', 'UserHourOfWeek', 'Country', 
                      'Region', 'Metro', 'City',
                      'DeviceType', 'OSFamily', 'OS', 'Browser', #'Recency',
                      'DeviceMake', 'DeviceModel', 'RenderingContext']
            
            
            
            # Find intersection between available features and standard features
            ColNames=list(set(df.columns) & set(ColNames))
            
            # Using features with more than 1 unique entry.
            ColNames=[col for col in ColNames if len(df[col].unique()) > 1 ]
                
            # Encode catgorical cols
            features,target=catboost_encoder(df[ColNames],df['engagement'])
            
            # Remove nan for mutual info extraction
            df=df.dropna(axis='columns')
            
            # Run univarate feature selection
            best_features=mutual_information(features,target,**kwargs)
            base_features.extend(best_features)
            if self.verbose>-1:
                print("-"*10,f'Chosen features are :',"-"*10)
                print(best_features)
            
            ColNames=list(set(base_features))
            
            
        elif model.lower()=='fpcpe':
            ColNames = base_features

        elif model.lower()=='fpctr':
            ColNames = base_features.extend[
                        'Frequency','Country',
                        'DeviceType','Browser','RenderingContext']
                        
        elif model.lower()=='fpcpa':
            ColNames = base_features
                        
        else:
            ColNames = base_features
        
        # if 'ColNames' in self.kwargs.keys():    
        #     ColNames = list(self.kwargs.get('ColNames',ColNames))
    
        if not ColNames:
            ColNames = base_features
        
        if self.verbose>1:
                    print("-"*10,f'Training dimensions to be used are',"-"*10)  
                    print(ColNames)
        print(f'ColNames = {ColNames}')           
        return ColNames

    def training_targets(self,model=None,**kwargs):
        '''
        The column names of labels/targets during training
        '''
        
        if self.verbose>-1:
            print("-"*10,f'Getting fp target features',"-"*10)
            
        if model is None:
            model = 'FP' #self.mlmodel        

        if model.lower()=='fpcpe':
            #cols = ['target','AdvertiserCurrency']
            cols = ['engagement','click','viewable','trackable', 'video-start',
                    'video-end','AdvertiserCurrency', 'impression', 'Cost']

        else:
            cols = ['engagement','click','viewable','trackable', 'video-start',
                    'video-end','converted','AdvertiserCurrency', 'impression', 'Cost']
            
        if self.verbose>-1:
            print("-"*10,f'Target features to be used are',"-"*10)
            print(cols)
            
        return cols

    def training_targets_beta(self,model=None,**kwargs):
        '''
        The column names of labels/targets during training
        '''
        if self.verbose>-1:
            print("-"*10,f'Getting beta target features',"-"*10)
            
        if model is None:
            model = self.mlmodel        

        cols = ['target','engagement','click','video-end',
                'viewability','converted','AdvertiserCurrency', 
                'impression', 'Cost']

        if self.verbose>0:
            print("-"*10,f'Target features to be used are',"-"*10)
            print(cols)
            
        return cols


    def groupby_features(self,**kwargs):
        '''
        The subset of the training featurs/columns we use
        to do groupby. 
        These columns define the hash that uniquely determine
        a single entry in the training.
        '''
        cols = self.training_dimensions(**kwargs)    
        return cols

    def feature_names(self,**kwargs):
        '''
        The name of all the columns used during modelling.
        This include both training and target columns.
        '''
        
                
        training_features = self.training_dimensions(**kwargs)
        print(f'TRAINING FEATURES AFTER TRAINING DIM ==> {training_features}')

        if kwargs.get('beta',False):
            training_targets = self.training_targets_beta(**kwargs)
        else:
            training_targets = self.training_targets(**kwargs)
        
        # Training features + Training targets
        training_features.extend(training_targets)
        
        if self.verbose>-1:
                    print("-"*10,f'Training + Target features to be used are',"-"*10)  
                    print(training_features)
                    
        print(f'TRAINING FEATURES AFTER TARGET ADDED ==> {training_features}')
                    
        return training_features

    def _bidlist_bidname(self):
        '''
        The name used to specify the bidvalue or bidfactor_value in a bidline
        '''
        bidname = "BidAdjustment"
        return bidname

    def model_dimensions(self,**kwargs):
        '''
        Here we collect the name of the columns that 
        make up the model. 
        Bidlist dimensions may require name mapping. 
        Note: model to bidlist name mapping is done by bidlist_dimensions()
        '''

        columns = self.training_dimensions(**kwargs)        
        columns.append(self.bidlist_bidname)        
        #remove unwanted columns 
        try:
            _ = columns.remove('target')
            _ = columns.remove('AdvertiserCurrency')
        except:
            pass

        return columns

    def bidlist_dimensions(self,**kwargs):
        pass

    def select_features(self,**kwargs):
        '''
        given dataframe which contains all features,
        return a subset of the features ensuring 
        * datatypes
        * above threshold target values
        * comment about imbalance 
        * diagnostic information
        '''
        
        
        df=kwargs.get('df').alias('df')
        # Define target if not already present.
        cols = df.columns
        if not 'target' in cols:
            df = df.withColumn("target", lit(0))
            if 'engagement' in cols:                
                df=df.withColumn("target", df.target+kwargs.get('engw',1)*df['engagement'])

            if 'click' in cols:
                df=df.withColumn("target", df.target+kwargs.get('clickw',1)*df['click'])

            if 'converted' in cols:
                df=df.withColumn("target", df.target+kwargs.get('convw',1)*df['converted'])
                
            if 'video-end' in cols:
                df=df.withColumn("target", df.target+kwargs.get('videow',1)*df['video-end'])
                
            if 'viewable' in cols:
                df=df.withColumn("target", df.target+kwargs.get('viewablew',1)*df['viewable'])
                
            if 'impression' in cols:
                df=df.withColumn("target", df.target+kwargs.get('impressionw',1)*df['impression'])
   
        # Get training and target columns 
        train_target = self.feature_names(**kwargs)
        
        # Get usable features
        features = []
        for n in train_target:
            if n in df.columns:
                features.append(n)
        
        # print('----'*20)
        # print(train_target)
        # print(features)

        # Select the desired features
        if self.verbose>-1:
            print("-"*10,f'Using the following features:',"-"*10)
            print(features)
        df = df[features]

        # Ensure proper dtypes for all columns
        dtype_dict,fill_dict = self.feature_dtypes(df.columns,fillval=True)
        
        # Warn incase we need to drop larger number of rows because of NaN
        # df = df.replace(r'^\s*$', np.NaN)
        dfclean = df.dropna(how='any')
        if self.verbose>-1:
            # if df.count()>dfclean.count():
            print("-"*10,f'WARNNING: Dropping missing rows with any missing values',"-"*10)
                # print('DataFrame rows before dropna: ',df.count())
                # print('DataFrame rows after dropna: ',dfclean.count())

        # Now set data types
        # df = dfclean.astype(dtype_dict)
            
        # Dignostic check
        try:
            pcount = df['target'].map(lambda x:x>0).sum()
            pfrac = kwargs.get('pfrac',0.1)        
            pfrac = float(pfrac)

            if self.verbose>-1:
                print("-"*10,f'CHecking for class imbalance',"-"*10)
                print(f'the fraction of positive targets is: {pfrac};  df.shape=',df.count())
                
                if pcount<1:
                    print('WARNNING: 100% class imbalance. Data is unusable for training.')

                elif pcount < pfrac*df.df.count():
                    print('WARNNING: high degree of imbalance in the data')

            else:
                pass

        except Exception as e:
            warnings.warn(f'Can not do diagnostic check because of error below')
            warnings.warn(str(e))
            
        if self.verbose>-1:
            print(df.printSchema())
            
        return df


    def all_params_json(self):
        jdict = {'mlmodel':self.mlmodel,
         'missingfrac':self.missingfrac,
         'maxrow':self.maxrow,
         'ml_columns':self.ml_columns,
         'data_columns':self.raw_log_columns,
         'config_params':self.config_params,
         'rootdir':self.rootdir,
         'maindir':self.main_dir,
         'bucket_name':self.bucket_name,
         'verbose':self.verbose}

        return jdict
