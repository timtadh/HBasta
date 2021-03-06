#!/usr/bin/env python
#
#  Licensed under the Apache License, Version 2.0 (the "License"); 
#  you may not use this file except in compliance with the License. 
#  You may obtain a copy of the License at 
#  
#      http://www.apache.org/licenses/LICENSE-2.0 
#     
#  Unless required by applicable law or agreed to in writing, software 
#  distributed under the License is distributed on an "AS IS" BASIS, 
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#  See the License for the specific language governing permissions and 
#  limitations under the License. 

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(  
    name         = 'hbasta',
    version      = '0.1',
    description  = 'HBase Thrift interface wrapper',
    author       = 'Adam Ever-Hadani & Tim Henderson',
    author_email = 'tim.tadh@gmail.com',
    url          = 'http://github.com/timtadh/hbasta',
    keywords     = ['hbasta', 'hbase', 'thrift'],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",        
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Text Processing :: Filters",
        "Topic :: Utilities",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators"        
        ],
    long_description = """\
HBasta Simple HBase Thrift wrapper to allow streamlined interaction
with hbase in python programs.
""",
    
    packages = find_packages(),

    include_package_data = False,    
   
    entry_points = {
        'console_scripts': [
            'hbase-prefix-scan = hbasta._cmd:prefix_scan',
        ]
    },
 
    zip_safe = True
)

