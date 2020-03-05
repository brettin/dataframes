import sys
import h5py
import pandas as pd
import numpy as np
import os
import sys
import gzip
import argparse

psr = argparse.ArgumentParser(description='count samples')
psr.add_argument('--in',  default='in_file.h5', help='inut h5 file')
args=vars(psr.parse_args())


store = pd.HDFStore(args['in'], mode='r')
print ("training samples: {:,}".format(store.get('/y_train').shape[0]))
print ("validation samples: {:,}".format(store.get('/y_val').shape[0]))
print ("test samples: {:,}".format(store.get('/y_test').shape[0]))

store.close()

