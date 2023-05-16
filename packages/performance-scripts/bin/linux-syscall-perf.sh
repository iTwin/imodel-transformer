#! /usr/bin/env bash
# get counts and time spent in syscalls for the program

strace -c $@

