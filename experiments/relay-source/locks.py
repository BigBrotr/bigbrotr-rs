"""Actual backend-incarnation lock barriers shared by the source laboratories."""
import json
import time
import lab


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def blocked(pg, db, waiter, holder):
    deadline = time.monotonic() + 2
    while True:
        result = json.loads(lab.sql(pg, db, f"""SELECT json_build_object(
            'pid',a.pid,'backend_start',a.backend_start::text,'blockers',pg_blocking_pids(a.pid),
            'holder_start',(SELECT backend_start::text FROM pg_stat_activity WHERE pid={holder['pid']}),
            'waiting_locks',(SELECT coalesce(json_agg(json_build_object('type',locktype,
                'mode',mode,'relation',relation::regclass::text)),'[]') FROM pg_locks
                WHERE pid=a.pid AND NOT granted)) FROM pg_stat_activity a
            WHERE a.pid={waiter['pid']} AND a.datname='{db}'"""))
        if result and holder['pid'] in result['blockers'] and result['waiting_locks']:
            require(result['backend_start'] == waiter['backend_start'] and
                result['holder_start'] == holder['backend_start'], 'lock-backend-incarnation')
            return result
        require(time.monotonic() < deadline, 'lock-barrier-not-observed')
        time.sleep(.01)

