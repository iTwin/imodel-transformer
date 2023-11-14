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
stack = []

for i, line in enumerate(sys.stdin):
    if line == "":
        continue
    if line[0] == " ":
        stack.append(line)
    else:
        full_stack = "".join(stack)
        if full_stack in stacks:
            stacks[full_stack] += 1
        else:
            stacks[full_stack] = 1
        stack.clear()
        # ignore the actual call since it contains varying pointers
        # stack.append(line) #ignore

for s, count in stacks.items():
    print(f"found {count}")
    print(s)

