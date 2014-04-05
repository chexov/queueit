set -xue
while true; do redis-cli --raw LPUSH q_in `date "+%s"`; sleep 1; done

