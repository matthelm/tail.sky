#!/bin/bash
TAIL_OPTS=""
if [[ $1 == "true" ]]; then
  TAIL_OPTS="-f"
fi
TAIL_OPTS+=" -n +0"
tail $TAIL_OPTS $2
