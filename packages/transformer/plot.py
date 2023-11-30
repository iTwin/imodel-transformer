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
^
(\t|[ ]{2}) # I screwed up one file and had to manually copy and paste
(?P<stat_name>
  (User|System)[ ]time[ ]\(seconds\)
| Elapsed[ ]\(wall[ ]clock\)[ ]time[ ]\(h:mm:ss[ ]or[ ]m:ss\)
| Maximum[ ]resident[ ]set[ ]size[ ]\(kbytes\)
):[ ]
(?P<value>.*)
$
""", re.VERBOSE)

strace_patt = re.compile(r"""
^
\s*?
(?P<timep>\S*)
\s*?
(?P<seconds>\S*)
\s*?
(?P<usecs_call>\S*)
\s*?
(?P<calls>\S*)
\s*?
(?P<errors>\S*)
\s*?
(?P<syscall>\S*)
$
""", re.VERBOSE)


# FIXME: use re.VERBOSE
file_patt = re.compile(r"""
^
(?P<version>[^_]+?)
(-(?P<commit>[a-z0-9]{7}))?
_
(?P<src>.+\.bim)
\.
(?P<type>usrtime|strace)
$
""", re.VERBOSE)

file_paths = {
  'Juergen.Hofer.Bad.Normals.bim': '/home/mike/work/Juergen.Hofer.Bad.Normals.bim',
  'bad-aspect-old.bim': '/home/mike/work/bad-aspect-old.bim',
  'shell-noobstruction.bim': '/home/mike/work/shell-noobstruction.bim',
}

def timeToSeconds(src: str):
  h, m, s = src.split(':') if src.count(':') == '2' else (0, *src.split(':'))
  t = timedelta(hours=float(h), minutes=float(m), seconds=float(s))
  return t.total_seconds()

cool_versions = {'oldtform': 'old', 'selectfrom': 'new'}
syscalls = {
  # 'pwrite64': {
  #   'color': 'cyan',
  #   'legend': 'pwrite64 ($s$)',
  # },
  # 'epoll_wait': {
  #   'color': 'gray',
  #   'legend': 'fsync ($s$)'
  # },
}

data = {}

for file in (*glob("*.usrtime"), *glob("*.strace")):
  parsed_file = file_patt.match(file)

  if not parsed_file:
    print('bad file name:', file)
    continue

  source_file = parsed_file.group('src')
  version = parsed_file.group('version')
  commit = parsed_file.group('commit')
  file_type = parsed_file.group('type')
  file_size_gb = os.stat(file_paths[source_file]).st_size / 1024**3

  if version not in cool_versions:
    continue

  if source_file not in data:
    data[source_file] = {
      'transforms': {},
      'size_gb': file_size_gb,
      'path': file_paths[source_file],
    }

  tform_data = data[source_file]['transforms'].get(version)
  if tform_data is None:
    tform_data = {
      'version': version,
      'commit': commit,
    }
    data[source_file]['transforms'][version] = tform_data

  start_patt = re.compile(r'^(\t|  )Command being timed' if file_type == 'usrtime' else r'% time')
  with open(file) as f:
    started = False
    for line in f:
      if not started:
        if start_patt.search(line):
          started = True
        continue

      # FIXME: would be faster to match all the stats at once in one regex

      if file_type == 'usrtime':
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

      elif file_type == 'strace':
        strace_match = strace_patt.match(line)
        if not strace_match:
          continue

        syscall = strace_match.group('syscall')

        for name in syscalls.keys():
          if syscall == name:
            tform_data[name] = float(strace_match.group('seconds'))

    if not started:
      raise Exception(f'never started file {file}')

  data[source_file]['transforms'][version] = tform_data

plt.figure(figsize=(10, 10))
plt.suptitle('transformation time and memory usage (lower is better)')
plt.subplots_adjust(bottom=0.2, hspace=0.5, wspace=0.5)

for i, (src, src_runs) in enumerate(data.items()):
  plt.subplot(1, 3, i + 1)
  plt.margins(x=0.2, y=0.2)
  plt.ylabel('ratio')
  plt.ylim(0, 1.5)

  with sqlite3.connect(src_runs['path']) as conn:
    class_count, = conn.execute('select count(*) from ec_Class').fetchone()

  # FIXME: add 
  plt.title('{}  {:.3g}GB/{:.2}kC'.format(src[0:10], src_runs['size_gb'], class_count/1000))
  cool_runs = [run for version, run in src_runs['transforms'].items() if version in cool_versions]

  versions = {k:v for k, v in sorted(src_runs['transforms'].items(), key=lambda t: t[0])
                  if k in cool_versions}

  group_count = 2
  group_bar_count = 2 + len(syscalls)
  group_width_ratio = 0.5
  bar_width = group_width_ratio / group_bar_count
  bar_offset = lambda t, center: center + (t - group_bar_count / 2) * bar_width + bar_width / 2

  plt.xticks([bar_offset(0.5, i) for i in range(len(versions))], [*cool_versions.values()], rotation=50)

  for j, (version, run) in enumerate(versions.items()):
    maxs = { key: max(run[key] for run in versions.values())
            # FIXME: centralize this list
             for key in ('max_rss', 'user_time', 'system_time', 'wall_clock_time', *syscalls.keys())}

    group_center = j
    group_bar_offset = lambda t: bar_offset(t, group_center)

    bars = []

    bars.append(plt.bar(
      group_bar_offset(0),
      run['wall_clock_time']/maxs['wall_clock_time'],
      label='wall clock time',
      align='center',
      width=bar_width,
      color='red',
    ))
    plt.bar_label(bars[-1], [run['wall_clock_time']], padding=2, fmt='{:.2g}', rotation='vertical')

    bars.append(plt.bar(
      group_bar_offset(0),
      run['user_time']/maxs['wall_clock_time'],
      label='user time',
      align='center',
      width=bar_width,
      color='blue',
    ))
    # plt.bar_label(user_time_bars, [run['user_time']], padding=2, fmt='{:.2g}', rotation='vertical')

    bars.append(plt.bar(
      group_bar_offset(0),
      run['system_time']/maxs['wall_clock_time'],
      label='system time',
      align='center',
      width=bar_width,
      color='orange',
    ))
    # plt.bar_label(sys_time_bars, [run['system_time']], padding=2, fmt='{:.2g}', rotation='vertical')

    bars.append(plt.bar(
      group_bar_offset(1),
      run['max_rss']/maxs['max_rss'],
      label='max rss',
      align='center',
      width=bar_width,
      color='purple',
    ))
    plt.bar_label(bars[-1], [run['max_rss']], padding=2, fmt='{:.2g}', rotation='vertical')

    for k, (syscall, syscall_cfg) in enumerate(syscalls.items()):
      bars.append(plt.bar(
        group_bar_offset(k + 2),
        run[syscall]/maxs[syscall],
        label=syscall_cfg['legend'],
        align='center',
        width=bar_width,
        color=syscall_cfg['color'],
      ))
      plt.bar_label(bars[-1], [run[syscall]], padding=2, fmt='{:.2g}', rotation='vertical')

    if i == 0 and j == 0:
      plt.legend(bars, [b._label + (' (s)' if 'time' in b._label else ' (KB)') for b in bars])

plt.savefig("graph.png")
plt.show()
