#!/usr/bin/env python3

import matplotlib.pyplot as plt
from subprocess import run
from glob import glob
import re
import os
from datetime import timedelta
import sqlite3

wall_clock_time_patt = re.compile(r"\tElapsed \(wall clock\) time \(h:mm:ss or m:ss\): (?P<value>.*)$")
system_time_patt = re.compile(r"\tSystem time \(seconds\): (?P<value>.*)$")
user_time_patt = re.compile(r"\tUser time \(seconds\): (?P<value>.*)$")
max_rss_patt = re.compile(r"\tMaximum resident set size \(kbytes\): (?P<value>.*)$")

usrtime_patt = re.compile(r"""
(\t|[ ]{2}) # I screwed up one file and had to manually copy and paste
(?P<stat_name>
  (User|System)[ ]time[ ]\(seconds\)
| Elapsed[ ]\(wall[ ]clock\)[ ]time[ ]\(h:mm:ss[ ]or[ ]m:ss\)
| Maximum[ ]resident[ ]set[ ]size[ ]\(kbytes\)
):[ ]
(?P<value>.*)$
""", re.VERBOSE)

# FIXME: use re.VERBOSE
file_patt = re.compile(r"^(?P<version>[^_]+?)(-(?P<commit>[a-z0-9]{7}))?_(?P<src>.+\.bim)\.usrtime$")

file_paths = {
  'Juergen.Hofer.Bad.Normals.bim': '/home/mike/work/Juergen.Hofer.Bad.Normals.bim',
  'bad-aspect-old.bim': '/home/mike/work/bad-aspect-old.bim',
  'shell-noobstruction.bim': '/home/mike/work/shell-noobstruction.bim',
}

def timeToSeconds(src: str):
  h, m, s = src.split(':') if src.count(':') == '2' else (0, *src.split(':'))
  t = timedelta(hours=float(h), minutes=float(m), seconds=float(s))
  return t.total_seconds()

data = {}

for file in glob("*.usrtime"):
  parsed_file = file_patt.match(file)

  if not parsed_file:
    print('bad file name:', file)
    continue

  source_file = parsed_file.group('src')
  version = parsed_file.group('version')
  commit = parsed_file.group('commit')
  file_size_gb = os.stat(file_paths[source_file]).st_size / 1024**3

  if source_file not in data:
    data[source_file] = {
      'transforms': {},
      'size_gb': file_size_gb,
      'path': file_paths[source_file],
    }

  tform_data = {
    'version': version,
    'commit': commit,
  }

  with open(file) as f:
    for line in f:
      # FIXME: would be faster to match all the stats at once in one regex

      usrtime_match = usrtime_patt.match(line)
      if not usrtime_match:
        continue

      stat_name = usrtime_match.group('stat_name')

      if stat_name.startswith('Elapsed'):
        tform_data['wall_clock_time'] = timeToSeconds(usrtime_match.group('value'))

      if stat_name.startswith('System time'):
        tform_data['system_time'] = float(usrtime_match.group('value'))

      if stat_name.startswith('User time'):
        tform_data['user_time'] = float(usrtime_match.group('value'))

      if stat_name.startswith('Maximum resident set size'):
        tform_data['max_rss'] = float(usrtime_match.group('value'))

  if 'wall_clock_time' not in tform_data:
    print('bad file:', file)
  else:
    data[source_file]['transforms'][version] = tform_data


plt.figure(figsize=(10, 10))
plt.suptitle('transformation time and memory usage (lower is better)')
plt.subplots_adjust(bottom=0.2, hspace=0.5, wspace=0.5)

cool_versions = {'oldtform', 'selectfrom'}

maxs = { key: max(run[key]
                   for src_runs in data.values()
                   for version, run in src_runs['transforms'].items()
                   if version in cool_versions)
               for key in ('max_rss', 'user_time', 'system_time', 'wall_clock_time')}

for i, (src, src_runs) in enumerate(data.items()):
  plt.subplot(1, 3, i + 1)
  plt.margins(x=0.2, y=0.2)
  plt.ylabel('ratio from max')
  plt.ylim(0, 1.3)

  with sqlite3.connect(src_runs['path']) as conn:
    class_count, = conn.execute('select count(*) from ec_Class').fetchone()

  # FIXME: add 
  plt.title('{}  {:.3g}GB/{:.2}kC'.format(src[0:10], src_runs['size_gb'], class_count/1000))
  cool_runs = [run for version, run in src_runs['transforms'].items() if version in cool_versions]

  versions = {k:v for k, v in sorted(src_runs['transforms'].items(), key=lambda t: t[0])
                  if k in cool_versions}

  group_count = 2
  group_bar_count = 4
  group_width_ratio = 0.8
  bar_width = group_width_ratio / group_bar_count
  bar_offset = lambda t, center: center + (t - group_bar_count / 2) * bar_width + bar_width / 2

  print([v for v in versions])
  plt.xticks([bar_offset(1.5, i) for i in range(len(versions))], versions.keys(), rotation=50)

  for j, (version, run) in enumerate(versions.items()):
    group_center = j
    group_bar_offset = lambda t: bar_offset(t, group_center)

    wall_clock_bars = plt.bar(
      group_bar_offset(0),
      run['wall_clock_time']/maxs['wall_clock_time'],
      label='wall clock time',
      align='center',
      width=bar_width,
      color='red',
    )
    plt.bar_label(wall_clock_bars, [run['wall_clock_time']], padding=2, fmt='{:.2g}', rotation='vertical')

    user_time_bars = plt.bar(
      group_bar_offset(1),
      run['user_time']/maxs['user_time'],
      label='user time',
      align='center',
      width=bar_width,
      color='blue',
    )
    plt.bar_label(user_time_bars, [run['user_time']], padding=2, fmt='{:.2g}', rotation='vertical')

    sys_time_bars = plt.bar(
      group_bar_offset(2),
      run['system_time']/maxs['system_time'],
      label='system time',
      align='center',
      width=bar_width,
      color='orange',
    )
    plt.bar_label(sys_time_bars, [run['system_time']], padding=2, fmt='{:.2g}', rotation='vertical')

    max_rss_bars = plt.bar(
      group_bar_offset(3),
      run['max_rss']/maxs['max_rss'],
      label='max rss',
      align='center',
      width=bar_width,
      color='purple',
    )
    plt.bar_label(max_rss_bars, [run['max_rss']], padding=2, fmt='{:.2g}', rotation='vertical')

    if i == 0 and j == 0:
      plt.legend(handles=[wall_clock_bars, user_time_bars, sys_time_bars, max_rss_bars])


plt.show()
plt.savefig("graph.png")
