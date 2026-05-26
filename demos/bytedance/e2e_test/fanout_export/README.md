# Ray HDFS Fan-out Export Online E2E

These configs validate `export.targets` on the online Ray E2E platform.

The scenarios intentionally use HDFS-backed input so Ray workers do not depend
on repository-local sample files. Most output paths include `{job_id}`; append
and `error_if_exists` rerun scenarios use a fixed run prefix so two separate
online jobs exercise the same target directory.
