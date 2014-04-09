#!/bin/bash

$PYTHON setup.py install

nosetests --nocapture tests/
