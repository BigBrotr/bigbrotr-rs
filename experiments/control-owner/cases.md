# Control backend exclusion acceptance cases

P01-T06/P02-T02, independent expectations, not executed evidence.
Run against the exact candidate Rust adapter and PostgreSQL 16.15. Verify all
changed rows externally and preserve every failed/unknown outcome.

1. Target 0007 appends after 0802. A populated legacy Control owner causes an
   atomic migration refusal: no new columns, receipt or row changes.
2. Missing activation, wrong dataset/instance/mode and invalid complete policy
   refuse acquisition without an owner/role effect.
3. First valid acquisition writes one Control resource, epoch 1, fresh role and
   incarnation, actual backend attribution. A simultaneous second client is busy,
   even with an expired scheduling timestamp.
4. Only the owning backend can renew or register Ingest. A second connection
   copying the complete owner tuple is refused, including after reconnect.
5. Killing the owning backend allows a successor on the same resource, epoch 2;
   epoch 1 becomes released/storage_fenced and its role retired. Resume/reconnect
   the original client and verify it cannot renew or register as epoch 1.
6. A guarded transaction retains exclusion after a session unlock until the
   transaction ends. A second session cannot take over during that interval.
7. A previously owning backend that explicitly released its session lock cannot
   mutate after a successor claim; PID/start or copied metadata cannot revive it.
8. Graceful release is atomic; failure before/after COMMIT followed by a fresh
   lock holder resolves actual history without a duplicate epoch or old authority.
9. Refuse changed backend attribution, multiple Control resources, concurrent
   unreleased Control epochs, retired roles and unqualified backend tuples.
10. After PostgreSQL crash/restart, a fresh owner acquires the same resource at the
    next epoch; full previous history survives. No native-writer claim follows.

The migration/catalog test starts RED with UnknownTarget(7). SQL/adapter cases
remain NOT_RUN until the exact-source native report records their actual outcomes.
