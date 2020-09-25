#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
xyz2MongoDB.py
Read .xyz file and efficiently write into MongoDB in parallel in Windows 10.

Ref:
- https://www.jianshu.com/p/7fdc66d40cfb
- https://docs.mongodb.com/manual/reference/geojson/

@date: Sep 24, 2020
"""

from __future__ import print_function
from __future__ import division
import os
import sys
import time
import argparse
import multiprocessing as mp
from geojson import Point, MultiPoint
from pyproj import CRS, Transformer
from collections import OrderedDict
from pymongo import MongoClient
import warnings
warnings.filterwarnings('ignore')

print("Number of processors: ", mp.cpu_count())
DB_CONN = MongoClient('mongodb://149.171.16.253:27017')['VDMS']

parser = argparse.ArgumentParser(description='Input and output data path', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-i', '--input', required=True, help='directory for input data')
parser.add_argument('-r', '--splitDir', default='./split_dir/', help='directory for splitted data')
parser.add_argument('-t', '--type', default='point', help='GeoJSON object type')
parser.add_argument('-b', '--block', default=3000, help='size of block for multipoint')
parser.add_argument('-c', '--collection', default='small_point', help='collection in MongoDB')
args = parser.parse_args(sys.argv[1:])

# Check input arguments validation
assert os.path.isfile(args.input), '%s not found' % args.input
if not os.path.exists(args.splitDir):
	os.mkdir(args.splitDir)

INSERT_ROW = 1000
FILE_SIZE = 1000000
TYPE = args.type
COLLECTION = args.collection
BLOCK = map(int, args.block)

collist = DB_CONN.list_collection_names()
if COLLECTION not in collist:
	if TYPE == 'point':
		COLLECTION = DB_CONN['small_point']
	elif TYPE == 'multipoint':
		COLLECTION = DB_CONN['small_multipoint']

input_crs = CRS.from_epsg(28356)
output_crs = CRS.from_epsg(4326)
transformer = Transformer.from_crs(input_crs, output_crs)

class XYZTOMongoDB:
	def __init__(self, file_path, split_dir):
		self.file_path = file_path
		self.split_dir = split_dir

	def gen_line(self):
		with open(self.file_path, mode='r', encoding='utf-8') as f:
			while(True):
				line = f.readline()
				if not line:
					break
				yield line

	def split_file(self, sp_row=1000000):
		"""
		Read file generator and split it per 100W rows in one file
		"""
		base_name = os.path.basename(self.file_path).replace('.', '_{}.')
		gen = self.gen_line()
		idx = 1
		while(True):
			split_name = base_name.format(str(idx)) # "bld3_{i}.xyz"
			try:
				with open("%s/%s" % (self.split_dir, split_name), mode='w', encoding='utf-8') as f:
					for i in range(sp_row):
						line = next(gen)
						f.write(line)
				idx += 1
			except StopIteration:
				break
		print('Splitting into %s files in total.' % idx)

	@staticmethod
	def gen_point_dict(sp_filepath):
		with open(sp_filepath, mode='r', encoding='utf-8') as f:
			if TYPE == 'multipoint':
				coord = list()
				heights = list()
				cnt = 0			
			while(True):
				line = f.readline()
				if not line:
					break
				lat, lon, height = map(float, line.split()[:3])
				if TYPE == 'multipoint':
					coord.append(tuple(transformer.transform(lat*0.2+336000, lon*0.2+6245250)))
					heights.append(height*0.2+20)
					cnt += 1				
					if cnt == BLOCK:
						d = OrderedDict()  # remember the inserting order
						d['geometry'] = MultiPoint(coord)
						d['height'] = heights
						cnt = 0
						coord.clear()
						heights.clear()
						yield d						
				elif TYPE == 'point':
					d = OrderedDict()
					d['geometry'] = Point(transformer.transform(lat*0.2+336000, lon*0.2+6245250))
					d['height'] = height*0.2+20
					yield d
			if TYPE == 'multipoint':
				d = OrderedDict()  # remember the inserting order
				d['geometry'] = MultiPoint(coord)
				d['height'] = heights
				yield d
			
	@property	
	def sp_file_list(self):
		"""
		Acquire all absolute path of all split files.
		"""
		abspath = os.path.abspath(self.split_dir) + "/"
		sp_filepath_list = list(map(lambda x: abspath+x, os.listdir(self.split_dir)))
		return sp_filepath_list

	@staticmethod
	def insert_mongodb(sp_filepath):
		"""
		Insert json into MongoDB with insert_many()
		"""
		sp_gen = XYZTOMongoDB.gen_point_dict(sp_filepath)
		coll = DB_CONN[COLLECTION]
		while(True):
			if TYPE == 'point':
				docs = []
				try:
					for i in range(INSERT_ROW):
						doc = next(sp_gen)
						docs.append(doc)
				except StopIteration:
					break
				finally:
					coll.insert_many(docs)
					# x = coll.insert_many(docs)
					# print(x.inserted_ids)
			elif TYPE == 'multipoint':
				coll.insert_one(sp_gen)
		print('Inserting into MongoDB.')

	def clean_split_files(self):
		"""
		Clean all split files.
		"""
		for sp_file in self.sp_file_list:
			os.remove(sp_file)
		print('Cleaning all split files in %s' % self.split_dir)


def run(file_path, split_dir):
	start = time.time()
	handler = XYZTOMongoDB(file_path, split_dir)
	handler.split_file(FILE_SIZE)
	sp_filepath_list = handler.sp_file_list
	pool = mp.Pool(mp.cpu_count())
	for file in sp_filepath_list:
		pool.apply_async(handler.insert_mongodb, (file,))
	pool.close()
	pool.join()	
	end = time.time()
	print('Writing MongoDB in {}s.'.format(round(end-start,3)))
	handler.clean_split_files()


if __name__ == '__main__':
	run(args.input, args.splitDir)
