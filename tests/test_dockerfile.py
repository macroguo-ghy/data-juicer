import unittest
from pathlib import Path


class DockerfileTest(unittest.TestCase):
    def test_runtime_image_installs_internal_io_extra(self):
        dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
        content = dockerfile.read_text()

        self.assertIn('"/app[internal_io]" "uvloop==0.21.0"', content)
        self.assertIn('"bytedance.metrics>=0.5.2,<1.0.0"', content)
        self.assertNotIn('"/app[distributed,internal_io]"', content)

    def test_runtime_image_installs_hive_deploy(self):
        dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
        content = dockerfile.read_text()

        self.assertIn("ssh sshpass wget bvc", content)
        self.assertLess(content.index("ssh sshpass wget bvc"), content.index("bvc clone -f dp/hive_deploy -C /opt/tiger"))
        self.assertIn("bvc clone -f dp/hive_deploy -C /opt/tiger", content)
        self.assertIn("JAVA_HOME=/opt/tiger/jdk/jdk1.8", content)
        self.assertIn("HADOOP_HOME=/opt/tiger/yarn_deploy/hadoop", content)
        self.assertIn("HADOOP_CONF_DIR=/opt/tiger/yarn_deploy/hadoop/conf", content)
        self.assertIn("HIVE_HOME=/opt/tiger/hive_deploy", content)
        self.assertIn("HIVE_CONF_DIR=/opt/tiger/hive_deploy/conf", content)
        self.assertIn("ARROW_LIBHDFS_DIR=/opt/tiger/yarn_deploy/hadoop/lib/native", content)
        self.assertIn("/etc/profile.d/data_juicer_hadoop_classpath.sh", content)
        self.assertIn('"${HADOOP_HOME}/bin/hadoop" classpath --glob', content)
        self.assertIn('export CLASSPATH="${HADOOP_CLIENT_CLASSPATH}:${HIVE_CONF_DIR:-/opt/tiger/hive_deploy/conf}', content)
        self.assertNotIn("ENV BASH_ENV=/etc/profile.d/data_juicer_hadoop_classpath.sh", content)
        self.assertIn("source /etc/profile.d/data_juicer_hadoop_classpath.sh", content)
        self.assertIn('test -n "$CLASSPATH"', content)
        self.assertIn('test -f "$HIVE_CONF_DIR/hive-site.xml"', content)
        self.assertNotIn("RUN apt-get install -y --no-install-recommends bvc", content)

    def test_runtime_image_installs_bytedray_with_hive_support(self):
        dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
        content = dockerfile.read_text()

        app_install = content.index('"$PYTHON_BIN" -m pip install "/app[internal_io]" "uvloop==0.21.0"')
        commented_vllm_install = content.index('# RUN pip3 install -i https://bytedpypi.byted.org/simple "vllm==0.8.5"')
        uninstall_ray = content.index("RUN pip3 uninstall ray -y && pip3 uninstall bytedray -y")
        bytedray_install = content.index('"bytedray[data,serve,default,bytedance,hive]~=2.46.0.0"')
        disabled_pip_check = content.index("# RUN pip3 check")

        self.assertLess(app_install, uninstall_ray)
        self.assertLess(app_install, commented_vllm_install)
        self.assertLess(commented_vllm_install, uninstall_ray)
        self.assertLess(uninstall_ray, bytedray_install)
        self.assertLess(bytedray_install, disabled_pip_check)
        self.assertNotIn('\nRUN pip3 install -i https://bytedpypi.byted.org/simple "vllm==0.8.5"', content)
        self.assertIn("# ARG SCM_RAY_CORE_PATH=1.0.0.4008", content)
        self.assertIn("# RUN wget http://luban-source.byted.org/repository/scm/inf.batch.ray.ray_core_", content)
        self.assertNotIn("\nARG SCM_RAY_CORE_PATH", content)
        self.assertNotIn("\nRUN wget http://luban-source.byted.org/repository/scm/inf.batch.ray.ray_core_", content)
        self.assertNotIn("RUN pip3 uninstall memray textual -y", content)
        self.assertNotIn('"rich==13.9.4" "pyarrow==21.0.0"', content)
        self.assertNotIn("\nRUN pip3 check", content)
        self.assertIn('("ray", "bytedray")', content)
        self.assertIn('md.packages_distributions().get("ray", [])', content)
        self.assertIn("No ray or bytedray distribution provides the ray module", content)
        self.assertIn("import aiohttp, attrs, bytedtqs, packaging, pyarrow, regex, typing_extensions", content)
        self.assertNotIn("import aiohttp, attrs, bytedtqs, packaging, pyarrow, regex, typing_extensions, vllm", content)
        self.assertIn("from bytedance import metrics", content)
        self.assertIn('print("bytedance.metrics", md.version("bytedance.metrics"))', content)
        self.assertIn('print("metrics.Client", metrics.Client)', content)
        self.assertNotIn('print("vllm", md.version("vllm"))', content)
        self.assertIn("ray module", content)
        self.assertIn("from ray.data.datasource.hive import HiveCatalog", content)

    def test_coverage_artifacts_are_ignored_locally_and_in_ray_packages(self):
        repo_root = Path(__file__).resolve().parents[1]

        for ignore_file in [".gitignore", ".rayignore"]:
            content = (repo_root / ignore_file).read_text()
            for pattern in [".coverage", ".coverage.*", "coverage.xml", "htmlcov/", "cov_annotate/"]:
                self.assertIn(pattern, content)
