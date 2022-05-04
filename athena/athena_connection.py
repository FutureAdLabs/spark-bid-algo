# imports
import time
import sys, os

# importing path
athena_path = f"{os.environ['HOME']}/etl-spark-athena/algos-common-scripts/"
sys.path.append(athena_path)

from test_code.scripts import fetch_for_purpose as fetch
    
class AthenConnection:
    
    def __init__(self, **kwargs):
        self.filter_by = kwargs.get('filter_by','AdvertiserId')
        self.filters = kwargs.get('filters',None)
        self.start_date = kwargs.get('start_date',None)
        self.end_date = kwargs.get('end_date', None)
        
    def connectToAthena(self):
        # 
    
    def getS3Path(self):
        
        path = fetch.fetch_for_purpose(date=[self.start_date, self.end_date], 
                                       filter={self.filter_by:self.filters})
        
        return path
    
    if __name__ == '__main__':
        c = athenConnection()
        c.get_s3_path()
        