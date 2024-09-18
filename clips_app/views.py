from django.shortcuts import render
from django.http import HttpResponse
from django_pandas.managers import DataFrameManager
import django_tables2 as tables
from IPython.display import HTML

# Create your views here.
import pyodbc
import sqlalchemy
import pandas as pd
import os
import pathlib
import sys
import pandas as pd

dname = os.path.dirname(os.path.abspath(__file__))
parent_dir = pathlib.Path(dname).parents[0]

utils = os.path.join(parent_dir,'utilities')
dev_test_data = os.path.join(parent_dir,'dev_test_data')


dev_test_data = os.path.join(dname,'dev_test_data')

utils = os.path.join(parent_dir,'utilities')
sys.path.insert(0, utils)

import utilities
from utilities.utils import sql_server

connection = sql_server(server = 'sql_server' , username = 'sa', port = 1443, password = 'tEST1234')
# print(connection.connection)

league_stats_df = connection.execute_query_return_df("""SELECT *

  FROM [lac_fullstack_dev].[dbo].[q2_final]
                                                     where teamName = 'LA Clippers'""", database_name='lac_fullstack_dev')

# print(test)
team_stats_df = connection.execute_query_return_df("""SELECT *
  FROM [lac_fullstack_dev].[dbo].[q4_d_final_all]
                                                   where team = 'LA Clippers'""", database_name='lac_fullstack_dev')



def league_stats(response):
    
    return render(response,  "clips_app/league_stats.html",{'table' : league_stats_df.to_html(index=False,escape=False, classes='table table-stripped')})

def team_stats(response):
    
    return render(response,  "clips_app/team_stats.html",{'table' : team_stats_df.to_html(index=False,escape=False, classes='table table-stripped')})
