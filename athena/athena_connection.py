# imports
import time
import sys, os

# clone etl-spark
import subprocess

def clone_repo(repo_name, branch_name="main"):
    if(not(os.path.exists(repo_name))):
        bashCommand = f"git clone -b {branch_name} https://${GIT_TOKEN}@github.com/FutureAdLabs/{repo_name}.git"
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        if(error):
            print("--- error cloning")
            print(error)
        print(output)
    else:
        print("--- repository already cloned")
                

# importing path
athena_path_old = f"{os.environ['HOME']}/etl-spark-athena/algos-common-scripts/"
clone_repo("etl-spark-athena", 'DS-678')
add_athena = "/athena" if "athena" not in os.getcwd() else ""
athena_path = f'{os.getcwd() + add_athena}/etl-spark-athena/algos-common-scripts/'
sys.path.append(athena_path)
sys.path.append(athena_path_old)
print(sys.path)

from test_code.scripts import fetch_for_purpose as fetch
    
class AthenConnection:
    
    def __init__(self, **kwargs):
        self.filter_by = kwargs.get('filter_by','AdvertiserId')
        self.filters = kwargs.get('filters',None)
        self.start_date = kwargs.get('start_date','2022-01-01')
        self.end_date = kwargs.get('end_date', '2022-01-02')
        
    def connectToAthena(self):
        # 
        pass
    
    def getS3Path(self):
        
        path = fetch.fetch_for_purpose(date=[self.start_date, self.end_date], 
                                       filter={self.filter_by:self.filters})

        print(path)

        return path
    
if __name__ == '__main__':
    c = AthenConnection(filter_by='campaign_id', filters=['fdbieo5'],
                        start_date='2022-01-10', end_date='2022-01-11')
    c.getS3Path()
    
    # clone_repo("etl-spark-athena")
    