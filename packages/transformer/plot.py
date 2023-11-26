#!/usr/bin/env python3

import matplotlib.pyplot as plt
from subprocess import run
from glob import glob
import re
from datetime import timedelta

wall_clock_time_patt = re.compile(r"\tElapsed \(wall clock\) time \(h:mm:ss or m:ss\): (?P<value>.*)$")
system_time_patt = re.compile(r"\tSystem time \(seconds\): (?P<value>.*)$")
user_time_patt = re.compile(r"\tUser time \(seconds\): (?P<value>.*)$")

# FIXME: use re.VERBOSE
file_patt = re.compile(r"^(?P<version>[^_]+?)(-(?P<commit>[a-z0-9]{7}))?_(?P<src>.+\.bim)\.usrtime$")

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

  if source_file not in data:
    data[source_file] = {
      'transforms': [],
    }

  tform_data = {
    'version': version,
    'commit': commit,
  }

  with open(file) as f:
    for line in f:
      wall_clock_match = wall_clock_time_patt.match(line)

      if wall_clock_match:
        tform_data['wall_clock_time'] = timeToSeconds(wall_clock_match.group('value'))

      system_time_match = system_time_patt.match(line)
      if system_time_match:
        tform_data['system_time'] = float(system_time_match.group('value'))

      user_time_match = user_time_patt.match(line)
      if user_time_match:
        tform_data['user_time'] = float(user_time_match.group('value'))

  if 'wall_clock_time' not in tform_data:
    print('bad file:', file)
  else:
    data[source_file]['transforms'].append(tform_data)


plt.figure(figsize=(10, 10))
plt.suptitle('transformation time (lower is better)')
plt.subplots_adjust(bottom=0.2)
plt.subplots_adjust(hspace=0.5)

for i, (src, src_runs) in enumerate(data.items()):
  plt.subplot(1, 3, i + 1)
  plt.ylabel('time (seconds)')
  plt.title(src)
  plt.xticks(rotation=50)
  print([run['version'] for run in src_runs['transforms']])
  cool_runs = [run for run in src_runs['transforms'] if run['version'] in {'oldtform', 'selectfrom'}]
  plt.bar(
    [run['version'] for run in cool_runs],
    [run['wall_clock_time'] for run in cool_runs],
  )

plt.show()
plt.savefig("graph.png")
