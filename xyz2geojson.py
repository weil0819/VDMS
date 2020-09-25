#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
xyz2geojson

Step-1: Transform EPSG:28356 to EPSG:4326
Step-2: Convert .xyz file into geojson file
Usage: python xyz2geojson.py --i='YOUR INPUT DIR' --o='YOUR OUTPUT DIR' --t='point/multipoint' --b=3000

Ref:
- https://geojson.org/
- https://pypi.org/project/geojson/
- http://pyproj4.github.io/pyproj/stable/

@date: Sep 23, 2020
@author: Wesley
"""

from __future__ import print_function
from __future__ import division
import os
import sys
import json
import argparse
from geojson import Point, MultiPoint
from pyproj import CRS, Transformer
from collections import OrderedDict
from config import stopwatch
import warnings
warnings.filterwarnings('ignore')



def argsParser():
	"""
	:return:
		- a dictionary the arguments
	"""
	parser = argparse.ArgumentParser(description='Input and output data path', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('-i', '--input', required=True, help='directory for input data')
	parser.add_argument('-o', '--output', required=True, help='directory for output data')
	parser.add_argument('-t', '--type', default='point', help='GeoJSON object type')
	parser.add_argument('-b', '--block', default=5000, help='size of block for multipoint')
	args = parser.parse_args(sys.argv[1:])

	return args


def xyz2point(inp, out, trans):
	"""
	to geojson "Point"
	"""
	jsonf = open(out, mode='w', encoding='utf-8')
	with open(inp, mode='r', encoding='utf-8') as f:
		while(True):
			line = f.readline().strip()
			print(line)
			if not line:
				break
			lat, lon, height = map(float, line.split()[:3])
			d = OrderedDict()  # remember the inserting order
			d['geometry'] = Point(trans.transform(lat*0.2+336000, lon*0.2+6245250))
			d['height'] = height*0.2+20
			jsonf.write(json.dumps(d, sort_keys=False, indent=4))
			jsonf.write('\n')
	jsonf.close()


def xyz2multipoint(inp, out, trans, block):
	"""
	to geojson "Multipoint"
	"""
	jsonf = open(out, mode='w', encoding='utf-8')
	block = map(int, block)
	with open(inp, mode='r', encoding='utf-8') as f:
		coord = list()
		heights = list()
		cnt = 0
		while(True):
			line = f.readline().strip()
			if not line:
				break
			lat, lon, height = map(float, line.split()[:3])
			coord.append(tuple(trans.transform(lat*0.2+336000, lon*0.2+6245250)))
			heights.append(height*0.2+20)
			cnt += 1
			if cnt == block:
				d = OrderedDict()  # remember the inserting order
				d['geometry'] = MultiPoint(coord)
				d['height'] = heights
				jsonf.write(json.dumps(d, sort_keys=False, indent=4))
				jsonf.write('\n')
				cnt = 0
				coord.clear()
				heights.clear()
		if coord:
			d = OrderedDict()  # remember the inserting order
			d['geometry'] = MultiPoint(coord)
			d['height'] = heights
			jsonf.write(json.dumps(d, sort_keys=False, indent=4))
			jsonf.write('\n')
	jsonf.close()


if __name__=='__main__':
	print('********** Initializing ArgumentParser and related arguments **********')
	args = argsParser()
	if not os.path.exists(args.input):
		sys.exit('Input file doesnot exist!!')

	print('********** Initializing Transformer **********')
	input_crs = CRS.from_epsg(28356)
	output_crs = CRS.from_epsg(4326)
	transformer = Transformer.from_crs(input_crs, output_crs)

	message = 'Opening "{}" and writing XYZ to Geojson'.format(args.input)	# output message
	with stopwatch(message):		
		if args.type == 'point':
			xyz2point(args.input, args.output, transformer)
		elif args.type == 'multipoint':
			xyz2multipoint(args.input, args.output, transfermer, args.block)
