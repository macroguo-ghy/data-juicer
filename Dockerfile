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
    apt-get install -y --no-install-recommends build-essential libgomp1 libxml2 ssh sshpass wget bvc; \
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

RUN rm -rf /opt/tiger/jdk && cd /opt/tiger && bvc clone jdk -f --version 1.0.0.19 && cd /opt/tiger/jdk && rm -rf jdk1* openjdk-11* openjdk-9* jdk_1.0.0.19.tar.gz && ln -s openjdk-1.8.0_265 jdk1.8 && chown -R tiger:tiger /opt/tiger/jdk
RUN bvc clone yarn_deploy /opt/tiger/yarn_deploy
RUN bvc clone -f dp/hive_deploy -C /opt/tiger # create /opt/tiger/hive_deploy

ENV JAVA_HOME=/opt/tiger/jdk/jdk1.8 \
    HADOOP_HOME=/opt/tiger/yarn_deploy/hadoop \
    HADOOP_CONF_DIR=/opt/tiger/yarn_deploy/hadoop/conf \
    YARN_CONF_DIR=/opt/tiger/yarn_deploy/hadoop/conf \
    HIVE_HOME=/opt/tiger/hive_deploy \
    HIVE_CONF_DIR=/opt/tiger/hive_deploy/conf \
    ARROW_LIBHDFS_DIR=/opt/tiger/yarn_deploy/hadoop/lib/native \
    HADOOP_COMMON_LIB_NATIVE_DIR=/opt/tiger/yarn_deploy/hadoop/lib/native \
    LD_LIBRARY_PATH=/opt/tiger/yarn_deploy/hadoop/lib/native:${LD_LIBRARY_PATH} \
    PATH=/opt/tiger/jdk/jdk1.8/bin:/opt/tiger/yarn_deploy/hadoop/bin:/opt/tiger/hive_deploy/bin:${PATH}

RUN set -eux; \
    printf '%s\n' \
        'if [ -n "${HADOOP_HOME:-}" ] && [ -x "${HADOOP_HOME}/bin/hadoop" ]; then' \
        '    HADOOP_CLIENT_CLASSPATH="$("${HADOOP_HOME}/bin/hadoop" classpath --glob 2>/dev/null || true)"' \
        'else' \
        '    HADOOP_CLIENT_CLASSPATH=""' \
        'fi' \
        'if [ -n "$HADOOP_CLIENT_CLASSPATH" ]; then' \
        '    export CLASSPATH="${HADOOP_CLIENT_CLASSPATH}:${HIVE_CONF_DIR:-/opt/tiger/hive_deploy/conf}:${HIVE_HOME:-/opt/tiger/hive_deploy}/lib/*${CLASSPATH:+:${CLASSPATH}}"' \
        'else' \
        '    export CLASSPATH="${HIVE_CONF_DIR:-/opt/tiger/hive_deploy/conf}:${HIVE_HOME:-/opt/tiger/hive_deploy}/lib/*${CLASSPATH:+:${CLASSPATH}}"' \
        'fi' \
        'unset HADOOP_CLIENT_CLASSPATH' \
        > /etc/profile.d/data_juicer_hadoop_classpath.sh; \
    chmod 0644 /etc/profile.d/data_juicer_hadoop_classpath.sh
RUN set -eux; \
    source /etc/profile.d/data_juicer_hadoop_classpath.sh; \
    java -version; \
    test -x "$JAVA_HOME/bin/java"; \
    test -x "$HADOOP_HOME/bin/hadoop"; \
    test -x "$HADOOP_HOME/bin/hdfs"; \
    test -x "$HIVE_HOME/bin/hive"; \
    test -n "$CLASSPATH"; \
    test -f "$HIVE_CONF_DIR/hive-site.xml"; \
    test -f "$ARROW_LIBHDFS_DIR/libhdfs.so"

COPY . /app

RUN set -eux; \
    PYTHON_BIN="$(command -v python3 || command -v python)"; \
    "$PYTHON_BIN" -m ensurepip --upgrade || true; \
    "$PYTHON_BIN" -m pip install --upgrade pip; \
    "$PYTHON_BIN" -m pip install "/app[internal_io]" "uvloop==0.21.0" "bytedance.metrics>=0.5.2,<1.0.0"

# vLLM is served outside the Ray job for now, so the runtime image does not
# install or validate vLLM directly.
# RUN pip3 install -i https://bytedpypi.byted.org/simple "vllm==0.8.5"

RUN pip3 uninstall ray -y && pip3 uninstall bytedray -y

# ARG SCM_RAY_CORE_PATH=1.0.0.4008
# RUN wget http://luban-source.byted.org/repository/scm/inf.batch.ray.ray_core_${SCM_RAY_CORE_PATH}.tar.gz \
#     && tar -xf inf.batch.ray.ray_core_${SCM_RAY_CORE_PATH}.tar.gz \
#     && ls | grep "cp310-cp310-manylinux2014" | xargs printf -- '%s[data,serve,default,bytedance,hive]\n' | xargs pip3 install -i https://bytedpypi.byted.org/simple \
#     && rm *.whl \
#     && rm inf.batch.ray.ray_core_${SCM_RAY_CORE_PATH}.tar.gz
RUN pip3 install -i https://bytedpypi.byted.org/simple "bytedray[data,serve,default,bytedance,hive]~=2.46.0.0"

# RUN pip3 check

RUN set -eux; \
    PYTHON_BIN="$(command -v python3 || command -v python)"; \
    "$PYTHON_BIN" -c 'import importlib.metadata as md; import aiohttp, attrs, bytedtqs, packaging, pyarrow, regex, typing_extensions; from bytedance import metrics; from data_juicer.config import init_configs; from data_juicer.ops.deduplicator.ray_document_deduplicator import RayDocumentDeduplicator; from data_juicer.ops.filter.audio_duration_filter import AudioDurationFilter; from data_juicer.ops.filter.audio_nmf_snr_filter import AudioNMFSNRFilter; from data_juicer.ops.mapper.download_file_mapper import DownloadFileMapper; import ray; from ray.data.datasource.hive import HiveCatalog; from pyiceberg.magnus import MagnusClient; import pyiceberg.magnus.lance_writer as lance_writer; print("py-data-juicer", md.version("py-data-juicer")); print("bytedance.metrics", md.version("bytedance.metrics")); print("metrics.Client", metrics.Client); ray_distribution = next((dist for dist in ("ray", "bytedray") if dist in md.packages_distributions().get("ray", [])), None); assert ray_distribution is not None, "No ray or bytedray distribution provides the ray module"; print(ray_distribution, md.version(ray_distribution)); print("ray module", ray.__file__); print("ray read_hive_table", getattr(ray.data, "read_hive_table", None)); print("HiveCatalog", HiveCatalog); print("data_juicer.config", init_configs.__module__); print("DownloadFileMapper", DownloadFileMapper); print("RayDocumentDeduplicator", RayDocumentDeduplicator); print("AudioDurationFilter", AudioDurationFilter); print("AudioNMFSNRFilter", AudioNMFSNRFilter); print("MagnusClient", MagnusClient); print("lance_writer", lance_writer.__file__)'
