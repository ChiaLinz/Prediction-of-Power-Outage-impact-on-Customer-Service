import os

class Config(object):
    DEBUG = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False

# For use on Heroku
class ProductionConfig(Config):
    SECRET_KEY = b'\xbbriA9(\xee/y\x1e\xc6\xb6\xcb\xff\x81\xda]\x19\x05\xe4\x9ez\x19\xa3' 
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')

# For use when developing on local machines
class DevelopmentConfig(Config):
    DEBUG = True
    SECRET_KEY = os.urandom(32)
    # Change to local database
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')