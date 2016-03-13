#!/bin/bash

set -e

scp $1 hadoop10@tenem11:~/data/coheel &
scp $1 hadoop10@tenem12:~/data/coheel &
scp $1 hadoop10@tenem13:~/data/coheel &
scp $1 hadoop10@tenem14:~/data/coheel &
scp $1 hadoop10@tenem15:~/data/coheel &
scp $1 hadoop10@tenem16:~/data/coheel &
scp $1 hadoop10@tenem17:~/data/coheel &
scp $1 hadoop10@tenem18:~/data/coheel &
scp $1 hadoop10@tenem19:~/data/coheel &
scp $1 hadoop10@tenem20:~/data/coheel &

let num=`ps ux | grep "scp $1 hadoop10@tenem" | wc -l`-1
while true; do
  echo -e "\nWaiting for $num parallel transfers to finish ..."
  sleep 5
  let num=`ps ux | grep "scp $1 hadoop10@tenem" | wc -l`-1
  if [ "$num" -le 0 ]; then
    break
  fi
done;

