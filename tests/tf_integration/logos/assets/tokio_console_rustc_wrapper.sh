#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
    printf '%s\n' 'Tokio-console rustc wrapper: missing compiler argument' >&2
    exit 64
fi

compiler=$1
shift
has_tokio_unstable=false
previous=

for argument do
    if [ "$previous" = '--cfg' ] && [ "$argument" = 'tokio_unstable' ]; then
        has_tokio_unstable=true
    fi
    if [ "$argument" = '--cfg=tokio_unstable' ]; then
        has_tokio_unstable=true
    fi
    previous=$argument
done

if [ "$has_tokio_unstable" = false ]; then
    set -- "$@" --cfg tokio_unstable
fi

rustc_wrapper=${LOGOS_BLOCKCHAIN_TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER-}
if [ -n "$rustc_wrapper" ]; then
    if [ "$rustc_wrapper" = "$0" ]; then
        printf '%s\n' 'Tokio-console rustc wrapper cannot wrap itself' >&2
        exit 64
    fi
    exec "$rustc_wrapper" "$compiler" "$@"
fi

exec "$compiler" "$@"
