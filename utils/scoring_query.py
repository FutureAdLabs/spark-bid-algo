# imports

import pandas as pd

from data import DataSources
# cleanBriefData, read_data_from_csv

class ScoringQuery(DataSources):
    
    def __init__(self, **kwargs):
        self.brief_and_ttd_matched = 'brief_and_ttd_matched'
        self.ttd_name_and_ids_matched = 'ttd_name_and_ids_matched'
        self.verticals_mapper = 'ttd_names_and_vertical_matched'
    
    def mergeData(self, **kwargs):
        # readFromCSV = DataSources().readFromCSV()
        ttd_brief_df = self.readFromCSV(self.brief_and_ttd_matched)
        # tdd_ids = self.readFromCSV(self.ttd_name_and_ids_matched)
        verticals_mapper = self.readFromCSV(self.verticals_mapper)
        self.sheetname = kwargs.get('sheetname','Sheet1')
        self.sheetid = kwargs.get('sheetid','1Ll-PkuW5zszhfcaotdhWOF6id_ok6HaUaDwpZe4OtZA')
        
        df_all_cmps = pd.merge(ttd_brief_df,
                               verticals_mapper,
                               how='inner',
                               left_on='ttd_campaign_name',
                               right_on='ttd_campaign_name')#.drop_duplicates()
        # print('df_all_cmps_________')                       
        # print(ttd_brief_df.info())
        # print(verticals_mapper.info())
        # print('')

        df_brief = self.cleanBriefData()

        df_matched = pd.merge(df_all_cmps, 
                              df_brief, 
                              how='inner', 
                              left_on=['brief_brand_name','brief_campaign_name'], 
                              right_on =['brief_brand_name','brief_campaign_name'])\
        .drop_duplicates()
        # print(df_matched.info())
        
        # df_final = pd.merge(final_df, 
        #                     verticals_mapper, 
        #                     how='inner', 
        #                     left_on='ttd_campaign_name', 
        #                     right_on='ttd_campaign_name')
        
        return df_matched
        
    def getIds(self, df, column=None, value=None):

        df = df.loc[df[column].isin(value)]            
        cmp = list(set(df.campaign_id))
        # print(cmp)    
        return cmp
            
    def getDateRange(self, df, column=None, value=None, start_date=None, end_date=None):
        
        date_range = []

        if start_date is None:
            start_date = list(set(df.loc[df[column].isin(value)].start_date))
            date_range.append(min(start_date).strftime('%Y-%m-%d'))
        else:
            try:
                date_range.append(start_date)
            except:
                print(f'{start_date} is not right date format')
        
        if end_date is None:
            end_date = list(set(df.loc[df[column].isin(value)].end_date))
            date_range.append(max(end_date).strftime('%Y-%m-%d'))
        else:
            try:
                date_range.append(end_date)
            except:
                print(f'{end_date} is not right date format')
        
        return date_range

    def getFilters(self, column=None, value=None):

        df = self.mergeData()
        if column is None:
            column = 'vertical'   
            if value is None:
                value = df[column].unique()
            else:
                print('Error msg')
        else:
            if value is None:
                value = df.column.unique()

        id = self.getIds(df, column, value)
        date_range = self.getDateRange(df, column, value)

        print(id, date_range)

        return (id, date_range)


if __name__ == "__main__":
    c1 = ScoringQuery()
    c1.getFilters()

 
            
        