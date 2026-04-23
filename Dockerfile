FROM hub.byted.org/base/debian.bookworm.python310:9e2c5d4d41f2e7dce6eeac497b78793a

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    MPLCONFIGDIR=/app/.mplconfig \
    DATA_JUICER_BUILD_EXTENSIONS=0 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_INDEX_URL=https://bytedpypi.byted.org/simple

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends build-essential libgomp1 libxml2 ssh sshpass; \
    if ! command -v sudo >/dev/null 2>&1; then \
        apt-get install -y --no-install-recommends sudo; \
    fi; \
    if ! id -u tiger >/dev/null 2>&1; then \
        useradd -u 1000 -d /home/tiger -m -s /bin/bash tiger; \
    fi; \
    mkdir -p /home/tiger/.service /opt/tiger /opt/log/tiger /var/log/tiger; \
    chown -R tiger:tiger /home/tiger /opt/tiger /opt/log/tiger /var/log/tiger; \
    printf '%s\n' \
        'Cmnd_Alias TIGER_COMMANDS = /usr/bin/svstat, /usr/bin/svc, /etc/init.d/nginx, /usr/bin/uwsgi, /usr/sbin/iotop, /sbin/setcap, /opt/tiger/bin/cgroups_root_util, /usr/sbin/tcpdump, /usr/bin/perf, /bin/echo_oom' \
        'tiger ALL=(ALL) NOPASSWD: TIGER_COMMANDS' \
        > /etc/sudoers.d/tiger; \
    printf '%s\n' 'tiger ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/sudoers; \
    chmod 0440 /etc/sudoers.d/tiger /etc/sudoers.d/sudoers; \
    rm -rf /var/lib/apt/lists/*

COPY . /app

RUN set -eux; \
    PYTHON_BIN="$(command -v python3 || command -v python)"; \
    "$PYTHON_BIN" -m ensurepip --upgrade || true; \
    "$PYTHON_BIN" -m pip install --upgrade pip; \
    "$PYTHON_BIN" -m pip install "/app[distributed]" "byted-iceberg[pyarrow]==0.2.360"; \
    "$PYTHON_BIN" -m pip check

RUN set -eux; \
    PYTHON_BIN="$(command -v python3 || command -v python)"; \
    "$PYTHON_BIN" -c 'import importlib.metadata as md; from data_juicer.config import init_configs; import ray; from pyiceberg.magnus import MagnusClient; import pyiceberg.magnus.lance_writer as lance_writer; print("py-data-juicer", md.version("py-data-juicer")); print("bytedray", md.version("bytedray")); print("ray module", ray.__file__); print("data_juicer.config", init_configs.__module__); print("MagnusClient", MagnusClient); print("lance_writer", lance_writer.__file__)'
