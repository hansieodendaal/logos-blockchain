#!/bin/sh

set -e

/usr/bin/logos-blockchain-faucet \
    --port $FAUCET_PORT \
    --node-base-url $NODE_API_ADDR \
    --faucet-pk $FAUCET_PK \
    --state-dir /faucet-data/private \
    --drip-amount 1000000000000

