

import sys
import h5py
import pandas as pd
import numpy as np
import os
import sys
import gzip
import argparse

psr = argparse.ArgumentParser(description='peek inside an h5 file')
psr.add_argument('--in',  default='in_file.h5', help='inut h5 file')
psr.add_argument('--key', default='df', help="h5 key")
args=vars(psr.parse_args())
print(args)



def h5_peek(infile):
    store = pd.HDFStore(infile, mode='r')
    l = store.keys()
    for i in range(len(l)):
        print ('key: ', l[i])
    store.close()

def info(infile, key):
    store = pd.HDFStore(infile, mode='r')
    df = store.get(key)
    print( 'shape of {}: {}: '.format(key,df.shape ))
    print ( 'size: ', df.size )
    store.close()

def traverse_datasets(hdf_file):
    def h5py_dataset_iterator(g, prefix=''):
        for key in g.keys():
            item = g[key]
            path = f'{prefix}/{key}'
            if isinstance(item, h5py.Dataset): # test for dataset
                yield (path, item)
            elif isinstance(item, h5py.Group): # test for group (go down)
                yield from h5py_dataset_iterator(item, path)

    with h5py.File(hdf_file, 'r') as f:
        for path, _ in h5py_dataset_iterator(f):
            yield path

def main(args):
    h5_peek(args['in'])
    for k in traverse_datasets(args['in']):
        print ('key: ', k)

    info(args['in'], args['key'])
    print('done')

if __name__ == '__main__':
    main(args)
