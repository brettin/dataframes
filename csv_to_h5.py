import sys
import h5py
import pandas as pd
import numpy as np
import os
import sys
import gzip
import argparse

psr = argparse.ArgumentParser(description='convert csv file to hdf5')
psr.add_argument('--in',  default='in_file.csv')
psr.add_argument('--save_dir', default=".")
psr.add_argument('--out', default='out_file.h5')
psr.add_argument('--key', default='df', help="h5 key")
args=vars(psr.parse_args())
print(args)

def load_csv_dataset(infile):
    print ('loading ', infile)
    chunksize = 100000
    chunks = pd.read_csv(infile, chunksize=chunksize, iterator=True)
    df = pd.concat(chunks, ignore_index=True)

    print ("shape of csv file: ", df.shape)
    return df

def save_df_to_h5(df, outfile='outfile.h5', key='df'):
    store = pd.HDFStore(outfile)
    store.append(key, df)
    return outfile

def csv_to_h5(infile, outfile='outfile.h5', key='df'):
    save_df_to_h5(load_csv_dataset(infile), outfile, key)


def main(args):
    csv_to_h5(args['in'], args['out'], args['key'])
    print('done')

if __name__ == '__main__':
    main(args)
