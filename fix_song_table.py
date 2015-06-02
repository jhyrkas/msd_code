import sys
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

letter = sys.argv[1]
f = open(letter + '/song_table.tsv')
f2 = open(sys.argv[2], 'w')

for line in f :
  ll = line.split('\t')
  for i in range(len(ll)) :
    if ll[i].strip() == 'nan' :
      c = columnNamesReordered[i]
      if c == 'artist_latitude' or c == 'artist_longitude' :
        f2.write('-200.0')
      elif c == 'artist_hotttnesss' or c == 'song_hotttnesss' or c == 'artist_familiarity' :
        f2.write('-1.0')
      else :
        print 'unknown column: ' + c
        exit(0)
    else :
      f2.write(ll[i].strip())
    if i < len(ll) - 1 :
      f2.write('\t')
    else :
      f2.write('\n')
