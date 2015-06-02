import h5py
import numpy as np
import beat_aligned_feats
import os
import glob
import sys

#f = h5py.File('data/A/A/A/TRAAAAW128F429D538.h5', 'r')
a = u'analysis'
met = u'metadata'
mb = u'musicbrainz'

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

fa_keys = [
 u'bars_confidence',
 u'bars_start',
 u'beats_confidence',
 u'beats_start',
 u'sections_confidence',
 u'sections_start',
 u'segments_confidence',
 u'segments_loudness_max',
 u'segments_loudness_max_time',
 u'segments_loudness_start',
 u'segments_pitches',
 u'segments_start',
 u'segments_timbre',
 u'songs',
 u'tatums_confidence',
 u'tatums_start'
]

fmet_keys = [
 u'artist_terms',
 u'artist_terms_freq',
 u'artist_terms_weight',
 u'similar_artists',
 u'songs'
]

fmb_keys = [
 u'artist_mbtags',
 u'artist_mbtags_count',
 u'songs'
]

pitch_schema = ['C', 'C_s', 'D', 'D_s', 'E', 'F',
                'F_s', 'G', 'G_s', 'A', 'A_s', 'B']
pitch_shifted_schema = ['pitch' + str(i) for i in range(12)]
timbre_schema = ['basis' + str(i) for i in range(12)]
segment_schema = ['song_id','segment_number','segment_start','segment_confidence',
                  'loudness_max','loudness_max_time','loudness_max_start'] + pitch_schema + timbre_schema

# 'universal' chroma values, from Krumhansl
k_major = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88])
k_minor = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17])
k_major = k_major / np.sum(k_major)
k_minor = k_minor / np.sum(k_minor)


def song_table_info(f) :
  vals = {}
  tmp = f[a][u'songs'].value[0]

  for i in range(len(tmp)) :
    if tmp.dtype.names[i] in song_table_columns :
      vals[tmp.dtype.names[i]] = str(tmp[i]).replace('\t', ' ')

  tmp = f[met][u'songs'][0]

  for i in range(len(tmp)) :
    if tmp.dtype.names[i] in song_table_columns :
      vals[tmp.dtype.names[i]] = str(tmp[i]).replace('\t', ' ')

  vals['year'] = f[mb][u'songs'][0][1]

  return vals

def mbtags_info(f) :
  rows = []
  tags = f[mb]['artist_mbtags'].value
  counts = f[mb]['artist_mbtags_count'].value
  song_id = f[met]['songs'][0][-3]
  for i in range(len(tags)) :
    vals = {'song_id' : song_id, 'mbtag' : tags[i], 'count' : counts[i]}
    rows.append(vals)

  return rows

mbtags_schema = ['song_id','mbtag','count']

def terms_info(f) :
  rows = []
  terms = f[met]['artist_terms'].value
  term_freqs = f[met]['artist_terms_freq'].value
  term_weights = f[met]['artist_terms_weight'].value
  song_id = f[met]['songs'][0][-3]
  for i in range(len(terms)) :
    vals = {'song_id' : song_id, 'term' : terms[i], 'freq' : term_freqs[i], 'weight' : term_weights[i]}
    rows.append(vals)

  return rows

terms_schema = ['song_id','term','freq','weight']

def similar_artists(f) :
  rows = []
  song_id = f[met]['songs'][0][-3]
  artist_id = f[met]['songs'][0][4]
  similar_artists = f[met]['similar_artists'].value
  for i in range(len(similar_artists)) :
    vals = {'song_id' : song_id, 'artist_id' : artist_id, 'similar_artist' : similar_artists[i]}
    rows.append(vals)

  return rows

similar_schema = ['song_id','artist_id','similar_artist']

def bars_info(f) :
  rows = []
  song_id = f[met]['songs'][0][-3]
  bars = f[a]['bars_start']
  confidence = f[a]['bars_confidence']
  for i in range(len(bars)) :
    vals = {'song_id' : song_id, 'bar_number' : i, 'bar_start' : bars[i], 'bar_confidence' : confidence[i]}
    rows.append(vals)

  return rows

bars_schema = ['song_id','bar_number','bar_start','bar_confidence']

def beat_info(f) :
  rows = []
  song_id = f[met]['songs'][0][-3]
  beats = f[a]['beats_start']
  confidence = f[a]['beats_confidence']
  for i in range(len(beats)) :
    vals = {'song_id' : song_id, 'beat_number' : i, 'beat_start' : beats[i], 'beat_confidence' : confidence[i]}
    rows.append(vals)
  
  return rows

beat_schema =[ 'song_id','beat_number','beat_start','beat_confidence']

def section_info(f) :
  rows = []
  song_id = f[met]['songs'][0][-3]
  sections = f[a]['sections_start']
  confidence = f[a]['sections_confidence']
  for i in range(len(sections)) :
    vals = {'song_id' : song_id, 'section_number' : i, 'section_start' : sections[i], 'section_confidence' : confidence[i]}
    rows.append(vals)
  
  return rows

section_schema = ['song_id','section_number','section_start','section_confidence']

def tatum_info(f) :
  rows = []
  song_id = f[met]['songs'][0][-3]
  tatums = f[a]['tatums_start']
  confidence = f[a]['tatums_confidence']
  for i in range(len(tatums)) :
    vals = {'song_id' : song_id, 'tatum_number' : i, 'tatum_start' : tatums[i], 'tatum_confidence' : confidence[i]}
    rows.append(vals)
  
  return rows

tatum_schema = ['song_id','tatum_number','tatum_start','tatum_confidence']

# TODO(hyrkas): this method could do something with pitch and timbre
def segment_info(f) :
  rows = []
  song_id = f[met]['songs'][0][-3]
  segment_start = f[a]['segments_start']
  confidence = f[a]['segments_confidence']
  loudness = f[a]['segments_loudness_max']
  loudness_time = f[a]['segments_loudness_max_time']
  loudness_start = f[a]['segments_loudness_start']
  timbre = f[a]['segments_timbre']
  pitches = f[a]['segments_pitches']

  for i in range(len(segment_start)) :
    vals = {'song_id' : song_id, 'segment_number' : i, 'segment_start' : segment_start[i],
      'segment_confidence' : confidence[i], 'loudness_max' : loudness[i],
      'loudness_max_time' : loudness_time[i], 'loudness_max_start' : loudness_start[i]}
    for j in range(12) :
      vals[pitch_schema[j]] = pitches[i][j]
      vals[timbre_schema[j]] = timbre[i][j]
    rows.append(vals)
  
  return rows

def beat_aligned_chroma(song_id, file_name) :
  btchromas = beat_aligned_feats.get_btchromas(file_name)
  if btchromas is None :
    return [], None
  btchromas = btchromas.T
  rows = []
  for i in range(btchromas.shape[0]) :
    vals = {'song_id': song_id, 'beat_number': i}
    for j in range(12) :
      vals[pitch_schema[j]] = btchromas[i][j]
    rows.append(vals)
  return rows, btchromas

def beat_aligned_timbre(song_id, file_name) :
  bttimbre = beat_aligned_feats.get_bttimbre(file_name)
  if bttimbre is None :
    return []
  bttimbre = bttimbre.T
  rows = []
  for i in range(bttimbre.shape[0]) :
    vals = {'song_id': song_id, 'beat_number': i}
    for j in range(12) :
      vals[timbre_schema[j]] = bttimbre[i][j]
    rows.append(vals)
  return rows

def pitch_shifted(song_id, beat_al_chrom):
  if beat_al_chrom is None :
    return []
  rows = []
  repr_pitch = np.sum(beat_al_chrom, 0) / np.max(sum(beat_al_chrom,0))
  max_rot = 0
  max_val = 0
  for i in range(12) :
    if np.roll(repr_pitch,i).dot(k_major) > max_val :
      max_rot = i
      max_val = np.roll(repr_pitch,i).dot(k_major)
    if np.roll(repr_pitch,i).dot(k_minor) > max_val :
      max_rot = i
      max_val = np.roll(repr_pitch,i).dot(k_minor)
  p = np.roll(beat_al_chrom, max_rot, axis=1)
  rows = []
  for i in range(beat_al_chrom.shape[0]) :
    vals = {'song_id': song_id, 'beat_number': i}
    for j in range(12) :
      vals[pitch_shifted_schema[j]] = beat_al_chrom[i][j]
    rows.append(vals)
  return rows



segment_schema = ['song_id','segment_number','segment_start','segment_confidence',
                  'loudness_max','loudness_max_time','loudness_max_start'] + pitch_schema + timbre_schema

beat_aligned_pitch_schema = ['song_id', 'beat_number'] + pitch_schema
beat_aligned_timbre_schema = ['song_id', 'beat_number'] + timbre_schema
pitch_shifted_table_schema = ['song_id', 'beat_number'] + pitch_shifted_schema

def run_analysis(files, output_dir) :
  count = 0
  
  # open with append in case of a crash
  song_table = open(output_dir + '/song_table.tsv', 'a', buffering=20*(1024**2))
  mbtags_table = open(output_dir + '/mbtags_table.tsv', 'a', buffering=20*(1024**2))
  terms_table = open(output_dir + '/terms_table.tsv', 'a', buffering=20*(1024**2))
  similar_artists_table = open(output_dir + '/similar_artist_table.tsv', 'a', buffering=20*(1024**2))
  bars_table = open(output_dir + '/bars_table.tsv', 'a', buffering=20*(1024**2))
  beats_table = open(output_dir + '/beats_table.tsv', 'a', buffering=20*(1024**2))
  sections_table = open(output_dir + '/sections_table.tsv', 'a', buffering=20*(1024**2))
  tatums_table = open(output_dir + '/tatums_table.tsv', 'a', buffering=20*(1024**2))
  
  segments_table = open(output_dir +'/segments_table.tsv', 'a', buffering=20*(1024**2))
  pitch_beat_aligned_table = open(output_dir +'/pitch_beat_aligned_table.tsv', 'a', buffering=20*(1024**2))
  timbre_beat_aligned_table = open(output_dir +'/timbre_beat_aligned_table.tsv', 'a', buffering=20*(1024**2))
  pitch_shifted_table = open(output_dir +'/pitch_transposed_table.tsv', 'a', buffering=20*(1024**2))

  song_buffer = ''
  mbtags_buffer = ''
  terms_buffer = ''
  similar_artists_buffer = ''
  bars_buffer = ''
  beats_buffer = ''
  sections_buffer = ''
  tatums_buffer = ''
  segments_buffer = ''
  pitch_buffer = ''
  timbre_buffer = ''
  transpose_buffer = ''
  for fi in files :
    file_name = fi.strip()
    print file_name
    f = h5py.File(file_name, 'r')
    song_id = f[met]['songs'][0][-3]

    #collect all values for all tables

    song_data = song_table_info(f)
    #if count == 0 :
    #  song_table.write('\t'.join(song_table_columns) + '\n')
    #song_table.write('	'.join(str(x) for x in song_data.values()) + '\n')
    
    mbtags = mbtags_info(f)
    #if count == 0 :
    #  mbtags_table.write('\t'.join(mbtags_schema) + '\n')
    

    terms = terms_info(f)
    #if count == 0 :
    #  terms_table.write('\t'.join(terms_schema) + '\n')

    similar = similar_artists(f)
    #if count == 0 :
    #  similar_artists_table.write('\t'.join(similar_schema) + '\n')
    

    bars = bars_info(f)
    #if count == 0 :
    #  bars_table.write('\t'.join(bars_schema) + '\n')

    beats = beat_info(f)
    #if count == 0 :
    #  beats_table.write('\t'.join(beat_schema) + '\n')

    sections = section_info(f)
    #if count == 0 :
    #  sections_table.write('\t'.join(section_schema) + '\n')

    tatums = tatum_info(f)
    #if count == 0 :
    #  tatums_table.write('\t'.join(tatum_schema) + '\n')

    segments = segment_info(f)
    #if count == 0 :
    #  segments_table.write('\t'.join(segment_schema) + '\n')

    f.close()

    pitch_aligned, btchroma = beat_aligned_chroma(song_id, file_name)
    #if count == 0 :
    #  pitch_beat_aligned_table.write('\t'.join(beat_aligned_pitch_schema) + '\n')
    

    timbre_aligned = beat_aligned_timbre(song_id, file_name)
    #if count == 0 :
    #  timbre_beat_aligned_table.write('\t'.join(beat_aligned_timbre_schema) + '\n')
    

    pitch_transpose = pitch_shifted(song_id, btchroma)
    #if count == 0 :
    #  pitch_shifted_table.write('\t'.join(pitch_shifted_table_schema) + '\n')

    
    # don't print until all values are collected, in case of a crash
    song_buffer += '\t'.join([str(song_data[song_table_columns[i]]) for i in range(len(song_table_columns))]) + '\n'
    #song_table.write('\t'.join([str(song_data[song_tables_columns[i]] for i in range(len(song_table_columns)))]) + '\n')
    for i in range(len(mbtags)) :
      mbtags_buffer += '\t'.join(str(mbtags[i][k]) for k in mbtags_schema) + '\n'
      #mbtags_table.write('\t'.join(str(mbtags[i][k]) for k in mbtags_schema) + '\n')

    for i in range(len(terms)):
      terms_buffer += '\t'.join(str(terms[i][k]) for k in terms_schema) + '\n'
      #terms_table.write('\t'.join(str(terms[i][k]) for k in terms_schema) + '\n')

    for i in range(len(similar)) :
      similar_artists_buffer += '\t'.join(str(similar[i][k]) for k in similar_schema) + '\n'
      #similar_artists_table.write('\t'.join(str(similar[i][k]) for k in similar_schema) + '\n')

    for i in range(len(bars)):
      bars_buffer += '\t'.join(str(bars[i][k]) for k in bars_schema) + '\n'
      #bars_table.write('\t'.join(str(bars[i][k]) for k in bars_schema) + '\n')

    for i in range(len(beats)):
      beats_buffer += '\t'.join(str(beats[i][k]) for k in beat_schema) + '\n'
      #beats_table.write('\t'.join(str(beats[i][k]) for k in beat_schema) + '\n')

    for i in range(len(sections)):
      sections_buffer += '\t'.join(str(sections[i][k]) for k in section_schema) + '\n'
      #sections_table.write('\t'.join(str(sections[i][k]) for k in section_schema) + '\n')

    for i in range(len(tatums)):
      tatums_buffer += '\t'.join(str(tatums[i][k]) for k in tatum_schema) + '\n'
      #tatums_table.write('\t'.join(str(tatums[i][k]) for k in tatum_schema) + '\n')

    for i in range(len(segments)):
      segments_buffer += '\t'.join(str(segments[i][k]) for k in segment_schema) + '\n'
      #segments_table.write('\t'.join(str(segments[i][k]) for k in segment_schema) + '\n')

    for i in range(len(pitch_aligned)) :
      pitch_buffer +=  '\t'.join(str(pitch_aligned[i][k]) for k in beat_aligned_pitch_schema) + '\n'
      #pitch_beat_aligned_table.write(
      #    '\t'.join(str(pitch_aligned[i][k]) for k in beat_aligned_pitch_schema) + '\n')

    for i in range(len(timbre_aligned)) :
      timbre_buffer += '\t'.join(str(timbre_aligned[i][k]) for k in beat_aligned_timbre_schema) + '\n'
      #timbre_beat_aligned_table.write(
      #    '\t'.join(str(timbre_aligned[i][k]) for k in beat_aligned_timbre_schema) + '\n')

    for i in range(len(pitch_transpose)) :
      transpose_buffer += '\t'.join(str(pitch_transpose[i][k]) for k in pitch_shifted_table_schema) + '\n'
      #pitch_shifted_table.write(
      #    '\t'.join(str(pitch_transpose[i][k]) for k in pitch_shifted_table_schema) + '\n')

    count += 1

    if count % 20 == 0 :
      song_table.write(song_buffer)
      mbtags_table.write(mbtags_buffer)
      terms_table.write(terms_buffer)
      similar_artists_table.write(similar_artists_buffer)
      bars_table.write(bars_buffer)
      beats_table.write(beats_buffer)
      sections_table.write(sections_buffer)
      tatums_table.write(tatums_buffer)
      segments_table.write(segments_buffer)
      pitch_beat_aligned_table.write(pitch_buffer)
      timbre_beat_aligned_table.write(timbre_buffer)
      pitch_shifted_table.write(transpose_buffer)
      song_buffer = ''
      mbtags_buffer = ''
      terms_buffer = ''
      similar_artists_buffer = ''
      bars_buffer = ''
      beats_buffer = ''
      sections_buffer = ''
      tatums_buffer = ''
      segments_buffer = ''
      pitch_buffer = ''
      timbre_buffer = ''
      transpose_buffer = ''
    
  song_table.write(song_buffer)
  mbtags_table.write(mbtags_buffer)
  terms_table.write(terms_buffer)
  similar_artists_table.write(similar_artists_buffer)
  bars_table.write(bars_buffer)
  beats_table.write(beats_buffer)
  sections_table.write(sections_buffer)
  tatums_table.write(tatums_buffer)
  segments_table.write(segments_buffer)
  pitch_beat_aligned_table.write(pitch_buffer)
  timbre_beat_aligned_table.write(timbre_buffer)
  pitch_shifted_table.write(transpose_buffer)
  song_table.close()
  mbtags_table.close()
  terms_table.close()
  similar_artists_table.close()
  bars_table.close()
  beats_table.close()
  sections_table.close()
  tatums_table.close()
  segments_table.close()
  pitch_beat_aligned_table.close()
  timbre_beat_aligned_table.close()
  pitch_shifted_table.close()

if __name__ == '__main__' :
  files = glob.glob(sys.argv[1] + '/*/*/*.h5')
  output_dir = sys.argv[2]
  run_analysis(files, output_dir)
