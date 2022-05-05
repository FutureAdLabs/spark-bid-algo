# imports
import numpy as np
import pandas as pd

# pyludio imports
import pyludio.rds as dbapi
import pyludio.ttd_api as tapi
from pyludio.gapi import gsheet

class DataSources:
    
    def __init__(self, **kwargs):
        self.sheetname = kwargs.get('sheetname','Sheet1')
        self.sheetid = kwargs.get('sheetid','1Ll-PkuW5zszhfcaotdhWOF6id_ok6HaUaDwpZe4OtZA')
        self.csv_path = kwargs.get('csv_path','Sheet1')
        
    def readFromSheet(self, **kwargs):
        """Reads data from gsheet.
        
        Returns:
            DataFrame

        """
        sheetname_campaign = self.sheetname
        sheetid = self.sheetid

        gs = gsheet(sheetid = sheetid)
        df = gs.get_sheet_df(name=sheetname_campaign)

        return df
    
    def cleanBriefData(self, df=None):
        """Changes column names, Cleans brief data.
        
        Args:
            df: Briefing dataframe
            
        Returns:
            Cleaned brief DataFrame
        """

        if df is None:
            df = self.readFromSheet() 
             
        rename_columns = {'End Date':'end_date', 
                          'Start Date':'start_date',
                          'Brand Name':'brief_brand_name',
                          'Campaign Name':'brief_campaign_name',
                          'Description':'description',
                          'Campaign Objectives':'campaign_objectives',
                          'KPIs':'kpis',
                          'Who Booked This? (brand/agency)':'brand_or_agency',
                          'Placement(s)':'placements',
                          'Target Audience':'target_audience',
                          'Serving Location(s)':'serving_locations',
                          'Black/white/audience list included?':'list_type',
                          'Cost Centre':'cost_center',
                          'Currency':'currency',
                          'Buy Rate (CPE)':'buy_rate',
                          'Volume Agreed':'volume_agreed',
                          'Gross Cost/Budget':'gross_cost',
                          'Agency Fee':'agency_fee',
                          'Percentage':'percentage',
                          'Net Cost':'net_cost',
                          'Is This a Standard Campaign?':'standard_campaign_or_not',
                          'Type of Client':'client_type'
                          } 
        df = df.rename(columns=rename_columns)
        # columns = ['running_days', 'end_date', 'start_date', 'brief_brand_name', 'brief_campaign_name', 'description', 
        #            'campaign_objectives', 'kpis', 'brand_or_agency', 'placements', 'target_audience', 
        #            'serving_locations','list_type', 'cost_center','currency','buy_rate', 'volume_agreed',
        #            'gross_cost', 'agency_fee', 'percentage', 'net_cost', 'standard_campaign_or_not', 'client_type']

        columns = list(rename_columns.values())

        df = df[columns]

        df['end_date'] = pd.to_datetime(df['end_date'])#, format='%m-%d-%Y')
        df['start_date'] = pd.to_datetime(df['start_date'])#, format='%m-%d-%Y')
        df['running_days'] = (df['end_date'] - df['start_date'])

        # removing columns with null values
        df = df[df['running_days'].notna()]
        df = df[df['serving_locations'].notna()]
        df = df[df['buy_rate'].notna()]
        df = df[df['gross_cost'].notna()]
        df = df[df['net_cost'].notna()]
        df = df[df['volume_agreed'].notna()]

        return df
    
    def readFromCSV(self, data=None):
        """Reads ttd ids and verticals matched data.
        
        Returns:
            DataFrame

        """

        if data == 'brief_and_ttd_matched':
            file = 'data/matched_briefing_ttd_names.csv'
            ttd_brief_matched_df = pd.read_csv(file) 

            rename_columns = {'Brand Name':'brief_brand_name', 
                              'Campaign Name':'brief_campaign_name',
                              'AdvertiserName':'advertiser_name',
                              'TTD Campaign Name':'ttd_campaign_name'
                             }
            ttd_brief_matched_df = ttd_brief_matched_df.rename(columns=rename_columns)

            return ttd_brief_matched_df
        
        elif data == 'ttd_name_and_ids_matched':
            file = 'data/cmp_match.csv'
            ids_matched_df = pd.read_csv(file)

            rename_columns = { 'campaign_name':'ttd_campaign_name'}
            ids_matched_df = ids_matched_df.rename(columns=rename_columns)

            return ids_matched_df
        
        elif data == 'ttd_names_and_vertical_matched':
            file = "data/consolidated_adv_cmp_vertical.csv"
            verticals_mapper = pd.read_csv(file)

            rename_columns = { 'campaign_name':'ttd_campaign_name'}
            verticals_mapper = verticals_mapper.rename(columns=rename_columns)

            verticals_mapper = verticals_mapper[['ttd_campaign_name', 'vertical']]

            return verticals_mapper
        
        else:
            df = pd.read_csv(self.csv_path)
            
            return df
    