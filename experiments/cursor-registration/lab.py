#!/usr/bin/env python3
"""Compatibility entrypoint for the existing cursor qualification profiles."""
import runpy
import sys
from pathlib import Path

sys.dont_write_bytecode = True
runpy.run_path(str(Path(__file__).resolve().parents[1]/'control-migrations/qualification.py'), run_name='__main__')
