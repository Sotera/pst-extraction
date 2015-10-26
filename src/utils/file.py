# -*- coding: utf-8 -*-
import os
import shutil
from functions import counter

def slurp(filePath):
    with open(filePath) as x: data = x.read()
    return data

# same as slurp but return Array of lines instead of string
def slurpA(filePath):
    with open(filePath) as x: data = x.read().splitlines()
    return data

def spit(filePath, data, overwrite=False):
    mode= 'w' if overwrite else 'a'
    with open(filePath, mode) as x: x.write(data)

def touch(filePath, times=None):
    with open(filePath, 'a'):
        os.utime(filePath, times)

def rm(filePath):
    if os.path.isfile(filePath):
        os.remove(filePath)

def rmrf(directory):
    ignore_errors = True
    shutil.rmtree(directory, ignore_errors)

def cp(src, dest):
    shutil.copyfile(src,dest)

def mv(src, dest):
    shutil.move(src,dest)

def mkdir(path):
    os.makedirs(path)

def mkdirp(path):
    if not os.path.exists(path):
        mkdir(path)


class RollingFile(object):

    def __init__(self, directory, filename, limit_megabytes=10):
        self.directory = directory
        self.filename = filename
        self.limit_bytes = limit_megabytes*1024*1024
        self.gen = counter()
        self.rotate = False

    def open(self):
        sz = "{}/{}-{:06d}".format(self.directory, self.filename, self.gen.next())
        self.f = open(sz, 'a+b')

    def rotate():
        self.rotate = True

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.f.close()
        
    def write(self,data):
        if self.rotate or (self.f.tell() > self.limit_bytes):
            self.rotate = False
            self.close()
            self.open()
        self.f.write(data)

