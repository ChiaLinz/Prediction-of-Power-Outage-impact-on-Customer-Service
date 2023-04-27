import argparse
from app import create_app

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dev", action="store_true")
args, unknown = parser.parse_known_args()
if args.dev:
    app = create_app('config.DevelopmentConfig')
else:
    app = create_app('config.ProductionConfig')

if __name__ == '__main__': 
    app.run()