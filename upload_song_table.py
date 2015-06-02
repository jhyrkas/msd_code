# -*- coding: utf-8 -*-

import sys
import myria
connection = myria.MyriaConnection(hostname='rest.myria.cs.washington.edu', port='1776', ssl=True)
sql_schema = \
  '''
  song_id text, analysis_sample_rate real, artist_7digitalid int, artist_familiarity real,
  artist_hotttnesss real, artist_id text, artist_latitude real, artist_location text,
  artist_longitude real, artist_mbid text, artist_name text, artist_playmeid int,
  audio_md5 text, danceability real, duration real, end_of_fade_in real, energy real, 
  key int, key_confidence real, loudness real, mode int, mode_confidence real,
  release text, release_7digitalid int, song_hotttnesss real, start_of_fade_out real,
  tempo real, time_signature int, time_signature_confidence real, title text,
  track_7digitalid int, track_id text, year int'''

columns = sql_schema.split(',')
columnNames = [] 
columnTypes = []

for c in columns :
  cc = c.split()
  columnNames.append(cc[0])
  if cc[1] == 'text' :
    columnTypes.append('STRING_TYPE')
  elif cc[1] == 'int' :
    columnTypes.append('LONG_TYPE')
  elif cc[1] == 'real' :
    columnTypes.append('DOUBLE_TYPE')
  else :
    print "can't interpret type " + cc[1]

print columnNames
print columnTypes

song_table_columns = [
 'analysis_sample_rate',
 'artist_7digitalid',
 'artist_familiarity',
 'artist_hotttnesss',
 'artist_id',
 'artist_latitude',
 'artist_location',
 'artist_longitude',
 'artist_mbid',
 'artist_name',
 'artist_playmeid',
 'audio_md5',
 'danceability',
 'duration',
 'end_of_fade_in',
 'energy',
 'key',
 'key_confidence',
 'loudness',
 'mode',
 'mode_confidence',
 'release',
 'release_7digitalid',
 'song_hotttnesss',
 'song_id',
 'start_of_fade_out',
 'tempo',
 'time_signature',
 'time_signature_confidence',
 'title',
 'track_7digitalid',
 'track_id',
 'year'
]

order = [columnNames.index(song_table_columns[i]) for i in range(len(song_table_columns))]
columnNamesReordered = [columnNames[i] for i in order]
columnTypesReordered = [columnTypes[i] for i in order]

schema = myria.MyriaSchema({"columnTypes" : columnTypesReordered, "columnNames" : columnNamesReordered})
#schema = {"columnTypes" : columnTypes, "columnNames" : columnNames}
destination = myria.MyriaRelation("Jeremy:MSD:SongsTable", 
                            schema=schema,
                            connection=connection)

directories = ['A', 'B', 'C', 'D', 'E', 'F', 'G',
               'H', 'I', 'J', 'K', 'L', 'M', 'N',
               'O', 'P', 'Q', 'R', 'S', 'T', 'U',
               'V', 'W', 'X', 'Y', 'Z']
filename_header = 'hdfs://vega:8020/user/hyrkas/msd_data/'
#filename_header = 'hdfs:///user/hyrkas/msd_data/'
filename = 'song_table.tsv'
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
