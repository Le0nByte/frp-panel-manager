#!/system/bin/sh
# Sample idempotent frpc start script (fixture, offline test only).
# Contract: safe to run repeatedly; kills any previous frpc, then starts one.
# The [ -x ] guard keeps this harmless in offline --fs-root test fixtures.
pkill -f /data/frp/frpc 2>/dev/null
if [ -x /data/frp/frpc ]; then
  /data/frp/frpc -c /data/frp/frpc.toml >/dev/null 2>&1 &
fi
exit 0
