import sqlite3

# table definitions

songs_table_sql = \
  '''
  CREATE TABLE songs (
    song_id text, analysis_sample_rate real, artist_7digitalid int, artist_familiarity real,
    artist_hotttnesss real, artist_id text, artist_latitude real, artist_location text,
    artist_longitude real, artist_mbid text, artist_name text, artist_playmeid int,
    audio_md5 string, danceability real, duration real, end_of_fade_in real, energy real, 
    key int, key_confidence real, loudness real, mode int, mode_confidence real,
    release text, release_7digitalid int, song_hotttnesss real, start_of_fade_out real,
    tempo real, time_signature int, time_signature_confidence real, title text,
    track_7digitalid int, track_id text, year int,
    PRIMARY KEY (song_id)
  );
  '''

bars_table_sql = \
  '''
  CREATE TABLE bars (
      song_id text, bar_number int, bar_start real, bar_confidence real,
      PRIMARY KEY (song_id, bar_number),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

beats_table_sql = \
  '''
  CREATE TABLE beats (
      song_id text, beat_number int, beat_start real, beat_confidence real,
      PRIMARY KEY (song_id, beat_number),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

mbtags_table_sql = \
  '''
  CREATE TABLE mbtags (
      song_id text, mbtag text, count int,
      PRIMARY KEY (song_id, mbtag),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

sections_table_sql = \
  '''
  CREATE TABLE sections (
      song_id text, section_number int, section_start real, section_confidence real,
      PRIMARY KEY (song_id, section_number),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

similar_artist_table_sql = \
  '''
  CREATE TABLE similar_artists (
      song_id text, artist_id text, similar_artist text,
      PRIMARY KEY (song_id, artist_id, similar_artist),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

tatums_table_sql = \
  '''
  CREATE TABLE tatums (
      song_id text, tatum_number int, tatum_start real, tatum_confidence real,
      PRIMARY KEY (song_id, tatum_number),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

terms_table_sql = \
  '''
  CREATE TABLE terms (
      song_id text, term text, freq real, weight real,
      PRIMARY KEY (song_id, term),
      FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

segments_table_sql = \
  '''
  CREATE TABLE segments (
    song_id text, segment_number int, segment_start real, segment_confidence real,
    loudness_max real, loudness_max_time real, loudness_max_start real, C real,
    C_s real, D real, D_s real, E real, F real, F_s real, G real, G_s real, A real,
    A_s real, B real, basis0 real, basis1 real, basis2 real, basis3 real, basis4 real,
    basis5 real, basis6 real, basis7 real, basis8 real,basis9 real, basis10 real, basis11 real,
    PRIMARY KEY (song_id, segment_number),
    FOREIGN KEY (song_id) REFERENCES songs(song_id)
  );
  '''

# l is a list of strings
# l is modified by this method
def song_table_data_types(l) :
  float_indeces = [1, 3, 4, 6, 8, 13, 14, 15, 16, 18, 19, 21, 24, 25, 26, 28]
  int_indeces = [2, 11, 17, 20, 23, 27, 30, 32]
  for i in float_indeces :
    l[i] = float(l[i])
  for i in int_indeces :
    l[i] = int(l[i])

# connect and create tables

conn = sqlite3.connect('/home/hyrkas/msd_subset/MillionSongSubset/database/msd.db')
conn.execute(bars_table_sql)
conn.execute(beats_table_sql)
conn.execute(mbtags_table_sql)
conn.execute(sections_table_sql)
conn.execute(tatums_table_sql)
conn.execute(segments_table_sql)
conn.execute(terms_table_sql)
conn.execute(songs_table_sql)
conn.execute(similar_artist_table_sql)
cur = conn.cursor()

# data insertion

print('inserting songs')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/song_table.tsv')
header = f.readline()
index = header.split('\t').index('song_id')
for line in f :
  ll = line.strip().split('\t')
  song_id = ll[index]
  del ll[index]
  ll = [song_id] + ll
  song_table_data_types(ll)
  sql = 'INSERT INTO songs values (' + ', '.join(['?'] * len(ll)) + ');'
  cur.execute(sql, tuple(ll))

f.close()
conn.commit()

print('inserting bars')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/bars_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO bars values (?, ?, ?, ?);',
              (ll[0], int(ll[1]), float(ll[2]), float(ll[3])))

f.close()
conn.commit()

print('inserting beats')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/beats_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO beats values (?, ?, ?, ?);',
              (ll[0], int(ll[1]), float(ll[2]), float(ll[3])))

f.close()
conn.commit()

print('inserting mbtags')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/mbtags_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO mbtags values (?, ?, ?);',
              (ll[0], ll[1], int(ll[2])))

f.close()
conn.commit()

print('inserting sections')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/sections_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO sections values (?, ?, ?, ?);',
              (ll[0], int(ll[1]), float(ll[2]), float(ll[3])))

f.close()
conn.commit()

print('inserting similar artists')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/similar_artist_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO similar_artists values (?, ?, ?);',
              (ll[0], ll[1], ll[2]))

f.close()
conn.commit()

print('inserting tatums')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/tatums_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO tatums values (?, ?, ?, ?);',
              (ll[0], int(ll[1]), float(ll[2]), float(ll[3])))

f.close()
conn.commit()

print('inserting terms')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/terms_table.tsv')
header = f.readline()
for line in f:
  ll = line.strip().split('\t')
  cur.execute('INSERT INTO terms values (?, ?, ?, ?);',
              (ll[0], ll[1], float(ll[2]), float(ll[3])))

f.close()
conn.commit()

print('inserting segments')

f = open('/home/hyrkas/msd_subset/MillionSongSubset/relational_data/segments_table.tsv')
header = f.readline()
for line in f :
  ll = line.strip().split('\t')
  ll[1] = int(ll[1])
  for i in range(2, len(ll)) :
    ll[i] = float(ll[i])
  sql = 'INSERT INTO segments values (' + ', '.join(['?'] * len(ll)) + ');'
  cur.execute(sql, tuple(ll))

f.close()
conn.commit()

print('load into sqlite complete')

# indeces

cur.execute('CREATE INDEX song_table_artist_id_idx on songs(artist_id, song_id);')
cur.execute('CREATE INDEX similar_artists_artist_id_idx on similar_artists(artist_id, similar_artist);')
cur.execute('CREATE INDEX terms_sorted_by_term on terms(term, song_id, freq, weight);')
cur.execute('CREATE INDEX segment_pitch_index on segments(song_id, segment_number, C,C_s,D,D_s,E,F,F_s,G,G_s,A,A_s,B);')
cur.execute('CREATE INDEX segment_timbre_index on segments(song_id, segment_number, basis0,basis1,basis2,basis3,basis4,basis5,basis6,basis7,basis8,basis9,basis10,basis11);')

# fixup artist_id issues by making the artist_id for artists with the exact same name
# be the minimum of all artist_id's associated with that name

cur.execute(
    '''
    CREATE TEMP VIEW min_artist_ids as 
    SELECT artist_name, min(artist_id) as artist_id, count(distinct artist_id) as id_count
    FROM songs
    GROUP BY artist_name HAVING id_count > 1 ORDER BY id_count DESC;
    '''
)

cur.execute(
    '''
    CREATE TEMP TABLE ids_to_fix as
    SELECT s.artist_id as old_id, m.artist_id as new_id
    FROM songs s, min_artist_ids m
    WHERE s.artist_name = m.artist_name
    GROUP BY old_id;
    '''
)

cur.execute(
    '''
    UPDATE songs
    '''
)
