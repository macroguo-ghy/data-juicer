# Ray HDFS Fan-out Export Online E2E

These configs validate `export.targets` on the online Ray E2E platform.

The scenarios intentionally use HDFS-backed input so Ray workers do not depend
on repository-local sample files. Most output paths include `{job_id}`; append
and `error_if_exists` rerun scenarios use a fixed run prefix so two separate
online jobs exercise the same target directory.

Checkpoint fan-out configs are append-only and at-least-once. A failed or
resubmitted job may skip checkpointed input rows, but it can still leave
duplicate part files if rows were written before Ray saved the sink checkpoint.

See [checkpoint_fanout_configs.md](checkpoint_fanout_configs.md) for the
checkpoint fan-out YAML details and validation commands.
