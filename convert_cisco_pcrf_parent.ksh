#!/usr/bin/ksh

BASE_DIR=/teoco/sa_root_med03
. $BASE_DIR/project/env/env.ksh

cd /teoco/implementation_med03/GD/scripts/Cisco_PCRF

/home/med03/.local/bin/pipenv run python $BASE_DIR/implementation/GD/scripts/Cisco_PCRF/convert_cisco_pcrf.py "/teoco/rdr_med03/raw_data/INPUTS/CISCO_PCRF/" &

