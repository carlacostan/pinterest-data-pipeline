from sqlalchemy import create_engine
import yaml

class AWSDBConnector:
    '''
    A connector to an AWS database containing Pinterest data.
    '''
    def __init__(self, db_config_path='/Users/carlajcostan/Documents/AICore/pinterest-data-pipeline892/db_creds.yml'):
        '''
        Constructs the necessary attributes for the AWSDBConnector object.
        '''
        with open(db_config_path, 'r') as file:
            db_config = yaml.safe_load(file)

        self.HOST = db_config['HOST']
        self.USER = db_config['USER']
        self.PASSWORD = db_config['PASSWORD']
        self.DATABASE = db_config['DATABASE']
        self.PORT = db_config['PORT']
        
    def create_db_connector(self):
        '''
        Uses the database type (mysql) and database API (pymysql) along with the
        previous attributes to create a connection to an AWS database containing Pinterest data.
        '''
        engine = create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

if __name__ == "__main__":
        connector = AWSDBConnector()
        engine = connector.create_db_connector()