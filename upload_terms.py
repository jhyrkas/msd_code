# -*- coding: utf-8 -*-

import sys
import myria
connection = myria.MyriaConnection(hostname='rest.myria.cs.washington.edu', port='1776', ssl=True)

columnNames = ['song_id', 'term', 'freq', 'weight']
columnTypes = ['STRING_TYPE', 'STRING_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE']

schema = myria.MyriaSchema({"columnTypes" : columnTypes, "columnNames" : columnNames})
#schema = {"columnTypes" : columnTypes, "columnNames" : columnNames}
destination = myria.MyriaRelation("Jeremy:MSD:TermsTable", 
                            schema=schema,
                            connection=connection)

directories = ['A', 'B', 'C', 'D', 'E', 'F', 'G',
               'H', 'I', 'J', 'K', 'L', 'M', 'N',
               'O', 'P', 'Q', 'R', 'S', 'T', 'U',
               'V', 'W', 'X', 'Y', 'Z']
filename_header = 'hdfs://vega:8020/user/hyrkas/msd_data/'
#filename_header = 'hdfs:///user/hyrkas/msd_data/'
filename = 'terms_table.tsv'
upload_files = []
for i in range(1,27) :
  f = filename_header + directories[i-1] + '/' + filename
  upload_files.append((i, f))
#fakefile = '/home/hyrkas/fakefile.tsv'
fakefile = 'file:///home/hyrkas/fakefile.tsv'
for i in range(27,73) :
  upload_files.append((i, fakefile))
print upload_files

scan_parameters = {'delimiter': '\t', 'quote': '᠎'}

query = myria.MyriaQuery.parallel_import(
          destination,
          upload_files,
          scan_parameters = scan_parameters
        )

#print query.to_dataframe()
