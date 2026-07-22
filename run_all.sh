#!/bin/bash
# Runs web/consumer/worker as three separate processes inside one container
# (single-container alternative to the split analytics-web/consumer/worker
# services). Each still gets its own log pair via configure_logging(args.mode)
# since every process calls `python main.py --mode <x>` independently.
set -e

python main.py --mode web &
python main.py --mode consumer &
python main.py --mode worker &

# Exit as soon as ANY one of the three dies, propagating its exit code, so a
# crashed sub-process actually surfaces as a container failure instead of the
# container silently running in a degraded state forever.
wait -n
exit $?
