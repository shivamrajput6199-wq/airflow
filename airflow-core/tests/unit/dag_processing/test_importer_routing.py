#
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

from unittest import mock

import pytest

from airflow.dag_processing.importer_routing import (
    _get_listed_file,
    get_claiming_coordinator,
    is_other_importer_file,
)
from airflow.sdk.importers import DagImportError, FilesystemDagDefinition

from unit.dag_processing.fake_importers import FAKE_IMPORTER, JAR_IMPORTER, task_sdk_importers, write_jar
from unit.dag_processing.fake_lang_sdk import FakeCoordinator, fake_coordinator


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        pytest.param("native.jar", "native.jar", id="relative-to-the-bundle"),
        pytest.param("native.jar/Main.java", "native.jar", id="archive-member"),
        pytest.param("{bundle}/native.jar", "native.jar", id="absolute-inside"),
        pytest.param("../outside.jar", None, id="relative-outside"),
        pytest.param("{root}/outside.jar", None, id="absolute-outside"),
        pytest.param("missing.jar", None, id="missing"),
    ],
)
def test_get_listed_file_of_a_discovery_error(tmp_path, reference, expected):
    bundle_path = tmp_path / "bundle"
    bundle_path.mkdir()
    write_jar(bundle_path / "native.jar", "native_dag")
    write_jar(tmp_path / "outside.jar", "outside_dag")
    error = DagImportError(source_reference=reference.format(bundle=bundle_path, root=tmp_path), message="x")

    assert _get_listed_file(error, bundle_path) == (bundle_path / expected if expected else None)


def test_get_listed_file_of_a_definition(tmp_path):
    jar = write_jar(tmp_path / "native.jar", "native_dag")

    assert _get_listed_file(FilesystemDagDefinition(jar), tmp_path) == jar


@pytest.mark.parametrize(
    ("path", "expected"),
    [("dags/native.jar", True), ("dags/native.fake", True), ("dags/dag.py", False), ("dags/dags.zip", False)],
)
def test_is_other_importer_file(path, expected):
    with task_sdk_importers(FAKE_IMPORTER, JAR_IMPORTER):
        assert is_other_importer_file("testing", path) is expected


def test_get_claiming_coordinator_returns_the_coordinator_of_its_importer(tmp_path):
    with fake_coordinator():
        coordinator = get_claiming_coordinator(tmp_path / "dags.native", "testing")
        others = [get_claiming_coordinator(tmp_path / name, "testing") for name in ("dags.other", "dag.py")]

    assert isinstance(coordinator, FakeCoordinator)
    assert others == [None, None]


@mock.patch("airflow.dag_processing.importer_routing._get_registry", autospec=True, return_value=None)
def test_get_claiming_coordinator_without_a_registry(mock_registry, tmp_path):
    assert get_claiming_coordinator(tmp_path / "dags.native", "testing") is None
