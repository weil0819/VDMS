#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
xyz2PostgreSQL.py
Read .xyz file and efficiently write into PostgreSQL in parallel.


@date: Sep 24, 2020
"""

from __future__ import print_function
from __future__ import division
import os
import sys
import json
import argparse
import multiprocessing as mp
from geojson import Point, MultiPoint
from pyproj import CRS, Transformer
from collections import OrderedDict
from config import stopwatch
import warnings
warnings.filterwarnings('ignore')

print("Number of processors: ", mp.cpu_count())


parser = argparse.ArgumentParser(description='Input and output data path', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-i', '--input', required=True, help='directory for input data')
parser.add_argument('-r', '--splitDir', required=True, help='directory for splitted data')
parser.add_argument('-t', '--type', default='point', help='GeoJSON object type')
parser.add_argument('-b', '--block', default=3000, help='size of block for multipoint')
parser.add_argument('-c', '--collection', default='small_flat', help='collection in MongoDB')
args = parser.parse_args(sys.argv[1:])
if not os.path.exists(args.input):
	sys.exit('Input file doesnot exist!!')



def run(file_path, split_path):



if __name__ == '__main__':
	run(args.input, args.splitDir)