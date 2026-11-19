# Faucet payment recovery

The faucet requires a private, persistent `--state-dir`. It keeps the complete
prepared payment in `pending.json` before publishing it. Each request supplies
a unique random 32-byte `Idempotency-Key` as hexadecimal; retries of one
request must reuse the same key. On restart, the worker
retries that exact transaction until it is confirmed. Completed records remain
in `completed-<idempotency key>.json` for recovery. A completed key cannot
produce another payment, even if a retry arrives after a restart.

These files contain recipient note openings. Only the faucet operator should
be able to read the directory. The public faucet endpoint accepts a public key
and does not authenticate its owner, so it does not provide the opening. An
operator must deliver the opening through an authenticated private channel.
Back up the state directory along with the faucet deployment.

If the process stops during the initial file write, startup can reject an
incomplete pending record. Inspect and repair that record before restarting;
do not remove a complete pending record while its transaction may still land.
