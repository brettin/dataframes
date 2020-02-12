import sys
import h5py
import pandas as pd
import numpy as np
import os
import sys
import gzip
import argparse

psr = argparse.ArgumentParser(description='convert h5 file to csv',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
psr.add_argument('--in',  default='in_file.h5', help='inut csv file')
psr.add_argument('--save_dir', default=".", help='dir to save the csv file in')
psr.add_argument('--out', default='out_file.csv', help='name of output csv file')
psr.add_argument('--key', default='df', help="h5 key")
args=vars(psr.parse_args())
print(args)

def load_h5_dataset(infile, key):
    print ('loading ', infile)
    df = pd.read_hdf(infile, key)
    print ("shape of h5 dataframe using key: ", args['key'], df.shape)
    return df

def h5_to_pandas(infile, partition):
    print ('converting h5 file to pandas')
    store = pd.HDFStore(infile, mode='r')
    df_y = store.select('y_{}'.format(partition))
    df_x = store.select('x_{}'.format(partition))
    return df_x, df_y

def save_df_to_csv(df, outfile='outfile.csv', key='df'):
    df.to_cvs(args['save_path'] + args['outfile'])

def h5_to_csv(infile, outfile='outfile.csv', key='df'):
    save_df_to_csv(load_h5_dataset(infile), outfile, key)


def main(args):
    # h5_to_csv(args['in'], args['save_dir'] + args['out'], args['key'])
    df = load_h5_dataset(args['in'], args['key'])
    save_to_csv(df, args['out'])
    print('done')

if __name__ == '__main__':
    main(args)
