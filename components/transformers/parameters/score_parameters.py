from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.sql import functions as f
from pyspark.sql.functions import lit,col,sum, avg, max, first, min, mean


class Verbose(Params):
    """Mixin for param minGroupSize: verbose."""

    verbose = Param(
        Params._dummy(),
        "verbose", "verbose status",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self):
        super(Verbose, self).__init__()
        self._setDefault(verbose=0)
        
    def getVerbose(self):
        """Gets the value of verbose or its default value. """
        return self.getOrDefault(self.verbose)
    
class ModelParams(Params):
    """Mixin for model parameters: model parameters."""

    modelParams = Param(
        Params._dummy(),
        "modelParams", "model parameters of entity used for scoring",
        typeConverter=TypeConverters.identity,
    )

    def __init__(self):
        super(ModelParams, self).__init__()
        
    def getModelParams(self):
        """Gets the model parameters dict. """
        return self.getOrDefault(self.modelParams)

class Config(Params):
    """Mixin for config:scoring config."""

    config = Param(
        Params._dummy(),
        "config", "configurations used for scoring",
        typeConverter=TypeConverters.identity,
    )

    def __init__(self):
        super(ModelParams, self).__init__()
        
    def getconfig(self):
        """Gets the model parameters dict. """
        return self.getOrDefault(self.config)

class MinScore(Params):
    """Mixin for param minScore: minimum score value."""

    minScore = Param(
        Params._dummy(),
        "minScore", "minimum score value for filtering",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self):
        super(MinScore, self).__init__()
        self._setDefault(minScore=0.0)
        
    def getMinScore(self):
        """Gets the value of minScore or its default value. """
        return self.getOrDefault(self.minScore)

class MinGroupSize(Params):
    """Mixin for param minGroupSize: minimum group size."""

    minGroupSize = Param(
        Params._dummy(),
        "minGroupSize", "minimum group size for filtering",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self):
        super(MinGroupSize, self).__init__()
        self._setDefault(minGroupSize=0)
        
    def getMinGroupSize(self):
        """Gets the value of minGroupSize or its default value. """
        return self.getOrDefault(self.minGroupSize)
    
class Beta(Params):
    """Mixin for param beta: bool to use beta scoring or not."""

    beta = Param(
        Params._dummy(),
        "beta", "boolean check to use beta scoring or not",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self):
        super(Beta, self).__init__()
        self._setDefault(beta=False)
        
    def getBeta(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.beta)
    
class KPI(Params):
    """Mixin for param KPI: list of kpi rates.""" 

    kpi = Param(
        Params._dummy(),
        "kpi", "list of kpi rates to compute.",
        typeConverter=TypeConverters.toListString,
    )

    def __init__(self):
        super(KPI, self).__init__()
        self._setDefault(kpi=[])
        
    def getKPI(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.kpi)
    
class ScoringFeatures(Params):
    """Mixin for param scoringFeatures: list of scoring features.""" 

    scoringFeatures = Param(
        Params._dummy(),
        "scoringFeatures", "list of scoring features to use.",
        typeConverter=TypeConverters.toListString,
    )

    def __init__(self):
        super(ScoringFeatures, self).__init__()
        self._setDefault(scoringFeatures=[])
        
    def getScoringFeatures(self):
        """Gets the value of scoringFeatures or its default value. """
        return self.getOrDefault(self.scoringFeatures)
    
    
class RSample(Params):
    """Mixin for param KPI: bool to use random sampling or not."""

    rSample = Param(
        Params._dummy(),
        "rSample", "boolean check to use random sampling for beta scoring.",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self):
        super(RSample, self).__init__()
        self._setDefault(rSample=False)
        
    def getRSample(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.rSample)
    
    
class KPIAggregation(Params):
    """Mixin for param KPI: dictionary of kpi aggregation rules."""

    kpiAggregation = Param(
        Params._dummy(),
        "kpiAggregation", "dictionary of kpi aggregation rules",
        typeConverter=TypeConverters.identity,
    )

    def __init__(self):
        super(KPIAggregation, self).__init__()        
        self._setDefault(kpiAggregation={'engagement' : sum("engagement").alias("engagement"),
                                         'click' : sum("click").alias("click"),
                                         'video-end' : sum("video-end").alias("video-end"),
                                         'video-start' : sum("video-start").alias("video-start"),
                                         'viewable' : sum("viewable").alias("viewable"),
                                         'trackable' : sum("trackable").alias("trackable"),
                                         'impression' : sum("impression").alias("impression"),
                                         'group_size' : sum("group_size").alias("group_size"),
                                         'target': sum("target").alias("target")
                                        })

    def getKPIS(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.kpiAggregation)

    
class FeatureAggregation(Params):
    """Mixin for param KPI: dictionary of feature aggregation rules."""

    featureAggregation = Param(
        Params._dummy(),
        "featureAggregation", "dictionary of feature aggregation rules",
        typeConverter=TypeConverters.identity,
    )

    def __init__(self):
        super(FeatureAggregation, self).__init__()
        self._setDefault(featureAggregation={'Recency': mean("Recency").alias("Recency"),
                                             'Frequency': mean("Frequency").alias("Frequency"),
                                             # 'LogEntryTime':'nunique',
                                             'Country': first("Country").alias("Country"),
                                             'AdvertiserId': first("AdvertiserId").alias("AdvertiserId"),
                                             'CampaignId': first("CampaignId").alias("CampaignId"),
                                             'AdgroupId': first("AdgroupId").alias("AdgroupId"),
                                             'AdFormat': first("AdFormat").alias("AdFormat"),
                                             'FoldPosition': first("FoldPosition").alias("FoldPosition"),
                                             'RenderingContext': first("RenderingContext").alias("RenderingContext"),
                                             'OS': first("OS").alias("OS"),
                                             'DeviceType': first("DeviceType").alias("DeviceType"),
                                             'Browser': first("Browser").alias("Browser"),
                                             'Site': first("Site").alias("Site")
                                            })
    def getFeatureAggregation(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.featureAggregation)

    
class CostAggregation(Params):
    """Mixin for param KPI: dictionary of cost aggregation rules."""

    costAggregation = Param(
        Params._dummy(),
        "costAggregation", "dictionary of cost aggregation rules",
        typeConverter=TypeConverters.identity,
    )

    def __init__(self):
        super(CostAggregation, self).__init__()
        self._setDefault(costAggregation={'AdvertiserCurrency':f.expr('percentile_approx(AdvertiserCurrency, 0.5)')\
                                          .alias('AdvertiserCurrency'),
                                          'Cost':f.expr('percentile_approx(Cost, 0.5)').alias('Cost'),
                                          'PartnerCostInUSDollars': f.expr('PartnerCostInUSDollars(PartnerCostInUSDollars, 0.5)')\
                                          .alias('PartnerCostInUSDollars'), 
                                          
                                         })

    def getKPIS(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.costAggregation)

    
class BoxPrice(Params):
    """Mixin for param KPI: bool to use percentile or not."""

    boxPrice = Param(
        Params._dummy(),
        "boxPrice", "boolean check to use percentile  for price calculation.",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self):
        super(BoxPrice, self).__init__()
        self._setDefault(boxPrice=False)

    def getBoxPrice(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.boxPrice)
    
class Clean(Params):
    """Mixin for param KPI: bool to use percentile or not."""

    clean = Param(
        Params._dummy(),
        "clean", "boolean check to use only selected features.",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self):
        super(Clean, self).__init__()
        self._setDefault(clean=False)

    def getBoxPrice(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.clean)
    

class ScoringFunction(Params):
    """Mixin for param inputCols: scoring function to be used."""

    scoringFunction = Param(
        Params._dummy(),
        "scoringFunction", "scoring function to be used.",
        typeConverter=TypeConverters.identity,
    )

    def __init__(self):
        super(ScoringFunction, self).__init__()
        self._setDefault(scoringFunction = None)

    def getInputCols(self):
        """Gets thescoring function or its default value. """
        return self.getOrDefault(self.scoringFunction)
    
class MutualInformation(Params):
    """Mixin for param KPI: bool to use percentile or not."""

    mutualInformation = Param(
        Params._dummy(),
        "mutualInformation", "boolean check to use mutual information for feature selection",
        typeConverter=TypeConverters.toBoolean,
    )

    def __init__(self):
        super(MutualInformation, self).__init__()
        self._setDefault(mutualInformation=False)

    def getMutualInformation(self):
        """Gets the value of beta or its default value. """
        return self.getOrDefault(self.mutualInformation)