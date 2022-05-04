# imports
from DataSources import cleanBriefData, read_data_from_csv

class Query:
    
    def __init__(self, **kwargs):
        self.brief_and_ttd_matched = kwargs.get('brief_and_ttd_matched')
        self.ttd_name_and_ids_matched = kwargs.get('ttd_name_and_ids_matched')
        self.verticals_mapper = kwargs.get('ttd_names_and_vertical_matched')
    
    def mergeData(self):
        ttd_brief_df = readFromCSV(self.brief_and_ttd_matched)
        tdd_ids = readFromCSV(self.ttd_name_and_ids_matched)
        verticals_mapper = readFromCSV(self.verticals_mapper)
        
        df1 = pd.merge(ttd_brief_df,
                       tdd_ids,
                       how='inner',
                       left_on='ttd_campaign_name',
                       right_on='ttd_campaign_name').drop_duplicates()
        
        brief_df = cleanBriefData()

        df2 = pd.merge(df1, 
                       brief_df, 
                       how='inner', 
                       left_on=['brief_brand_name','brief_campaign_name'], 
                       right_on =['brief_brand_name','brief_campaign_name'])\
        .drop_duplicates()
        
        return df_all_cmp
        
    def query(filter=None):
        