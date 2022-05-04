# imports
from DataSources import cleanBriefData, read_data_from_csv

class ScoreQuery:
    
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
        
        return df2
        
    def query(self, filter=None):
        df = mergeData().groupby(filter)

        return df

    def getIds(self, df, ver):
        # for ver in df.vertical.unique():
        ads = list(set(df.loc[df['vertical'] == ver].advertiser_id))
        # print(f'{ver} : {ads}')
        
        return ads

    def getDates(self, df, ver):
        date_range = []
        # for ver in df.vertical.unique():
        start_date = list(set(df.loc[df['vertical'] == ver].start_date))
        date_range.append(min(start_date).strftime('%Y-%m-%d'))
        
        end_date = list(set(df.loc[df['vertical'] == ver].end_date))
        date_range.append(max(end_date).strftime('%Y-%m-%d'))
        
        return date_range
        