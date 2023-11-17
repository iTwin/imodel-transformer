"""
analyze unique stacks of `strace -k` call
# I did:
```sh
strace -k -e trace=/write
```
"""

import sys
from pprint import pprint

stacks = {}
curr_call = ""
stack = []

for i, line in enumerate(sys.stdin):
    if line == "":
        continue
    if line[0] == " ":
        stack.append(line)
    else:
        full_stack = "".join(stack)
        if full_stack in stacks:
            stacks[full_stack]['count'] += 1
            if (curr_call.startswith('mmap(')):
                mmap_size = curr_call.split(',')[1].strip()
                stacks[full_stack]['mmap_size'].append(mmap_size)
        else:
            stacks[full_stack] = { 'count': 1, 'mmap_size': [] }
        stack.clear()
        # ignore the actual call since it contains varying pointers
        # stack.append(line) #ignore
        curr_call = line

for s, item in sorted(stacks.items(), key=lambda t: t[1]['count']):
    print(f"found {item['count']}")
    print(f"mmap_sizes: {','.join(item['mmap_size'])}")
    print(s)

