# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import zipfile

import pytest

from airflow.dag_processing.importers import PythonDagImporter


class TestPythonDagImporterListDagFiles:
    @pytest.mark.parametrize("archive_name", ["task_handler.jar", "no_suffix"])
    def test_skips_zip_archive_without_zip_suffix(self, tmp_path, archive_name):
        dag_source = "from airflow.sdk import DAG\n"
        (tmp_path / "dag.py").write_text(dag_source)
        (tmp_path / "upper_dag.PY").write_text(dag_source)
        for name in ("lower.zip", "upper.ZIP", archive_name):
            with zipfile.ZipFile(tmp_path / name, "w") as zf:
                zf.writestr("dag.py", dag_source)

        assert set(PythonDagImporter().list_dag_files(tmp_path)) == {
            str(tmp_path / name) for name in ("dag.py", "upper_dag.PY", "lower.zip", "upper.ZIP")
        }
