""" import paths paths in notebooks to get paths setup properly """
import sys
import os
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
os.chdir('..')

