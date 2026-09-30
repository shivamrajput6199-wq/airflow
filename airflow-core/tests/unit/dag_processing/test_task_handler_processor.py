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

import os
import time
from unittest.mock import MagicMock, patch

import pytest
import structlog
from pydantic import TypeAdapter

from airflow.dag_processing.processor import (
    DagFileParseRequest,
    DagFileParsingResult,
    DagFileProcessorProcess,
    TaskHandlerDeclaration,
    TaskHandlerParam,
    TaskHandlerParsingResult,
    ToDagProcessor,
    ToManager,
)
from airflow.dag_processing.task_handler_processor import SDKTaskHandlerProcessorProcess
from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.exceptions import AirflowRuntimeError
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import CommsDecoder, GetVariable

from unit.dag_processing.fake_lang_sdk import FakeCoordinator, fake_coordinator, play_runtime, write_artifact


@pytest.fixture(autouse=True)
def _coordinator():
    with fake_coordinator():
        yield


def _run(tmp_path, *, coordinator: str = "fake", dag_ids=("etl",)) -> TaskHandlerParsingResult:
    return SDKTaskHandlerProcessorProcess.run(
        coordinator=coordinator,
        path=write_artifact(tmp_path / "etl.artifact"),
        bundle_path=tmp_path,
        bundle_name="task-handlers",
        artifact_rel_path="etl.artifact",
        dag_ids=dag_ids,
        logger=structlog.get_logger(),
    )


@patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
def test_asks_the_runtime_for_the_handlers_of_the_requested_dags(mock_parse_task_handler, tmp_path):
    def reply(request, comms):
        # Echo what the request carries, so the test can check it.
        param = TaskHandlerParam(name=request.bundle_name, value_schema={"type": "string"}, required=True)
        return TaskHandlerParsingResult(
            fileloc=request.file,
            task_handlers={
                dag_id: [TaskHandlerDeclaration(task_id=os.fspath(request.bundle_path), params=[param])]
                for dag_id in request.dag_ids
            },
        )

    mock_parse_task_handler.side_effect = play_runtime(reply)

    result = _run(tmp_path, dag_ids=iter(["etl", "report"]))

    param = TaskHandlerParam(name="task-handlers", value_schema={"type": "string"}, required=True)
    declaration = TaskHandlerDeclaration(task_id=os.fspath(tmp_path), params=[param])
    assert result == TaskHandlerParsingResult(
        fileloc=os.fspath(tmp_path / "etl.artifact"),
        task_handlers={"etl": [declaration], "report": [declaration]},
    )


def test_a_coordinator_that_is_not_configured_is_an_import_error(tmp_path):
    result = _run(tmp_path, coordinator="missing")

    assert result == TaskHandlerParsingResult(
        fileloc=os.fspath(tmp_path / "etl.artifact"),
        task_handlers={},
        import_errors={
            "etl.artifact": "Cannot start the Lang-SDK runtime: "
            "InvalidCoordinatorError: No coordinator 'missing' in [sdk] coordinators"
        },
    )


@patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
def test_a_dag_file_parsing_result_is_not_a_task_handler_result(mock_parse_task_handler, tmp_path):
    def reply(request, comms):
        with pytest.raises(AirflowRuntimeError, match="Unhandled request"):
            comms.send(DagFileParsingResult(fileloc=request.file, serialized_dags=[]))

    mock_parse_task_handler.side_effect = play_runtime(reply)

    result = _run(tmp_path)

    assert result.import_errors == {
        "etl.artifact": "The Lang-SDK runtime exited with code 0 without a parse result"
    }


def _probe_from_a_dag_parsing_child() -> None:
    """Stand in for ``_parse_file_entrypoint``: probe the file it is asked to parse, and return the result."""
    comms_decoder = CommsDecoder[ToDagProcessor, ToManager](body_decoder=TypeAdapter(ToDagProcessor))
    request = comms_decoder._get_response()
    assert isinstance(request, DagFileParseRequest)
    task_runner.SUPERVISOR_COMMS = comms_decoder  # type: ignore[assignment]

    result = SDKTaskHandlerProcessorProcess.run(
        coordinator="fake",
        path=request.file,
        bundle_path=request.bundle_path,
        bundle_name=request.bundle_name,
        artifact_rel_path="etl.artifact",
        dag_ids=["etl"],
        logger=structlog.get_logger(logger_name="task"),
    )
    comms_decoder.send(
        DagFileParsingResult(
            fileloc=request.file, serialized_dags=[], warnings=[result.model_dump(mode="json")]
        )
    )


@patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
def test_a_runtime_request_is_answered_by_the_dag_processor(mock_parse_task_handler, tmp_path):
    def reply(request, comms):
        variable = comms.send(GetVariable(key="probe_var"))
        return TaskHandlerParsingResult(
            fileloc=request.file,
            task_handlers={"etl": [TaskHandlerDeclaration(task_id=variable.value, params=[])]},
        )

    mock_parse_task_handler.side_effect = play_runtime(reply)
    client = MagicMock(spec=Client)
    client.variables = MagicMock()
    client.variables.get.return_value = VariableResponse(key="probe_var", value="from-the-dag-processor")
    artifact = write_artifact(tmp_path / "etl.artifact")

    proc = DagFileProcessorProcess.start(
        id=1,
        path=artifact,
        bundle_path=tmp_path,
        bundle_name="task-handlers",
        dag_file_rel_path="etl.artifact",
        callbacks=[],
        target=_probe_from_a_dag_parsing_child,
        logger=structlog.get_logger(),
        logger_filehandle=MagicMock(),
        client=client,
    )
    deadline = time.monotonic() + 30
    while not proc.is_ready:
        assert time.monotonic() < deadline, "the Dag-parsing child did not finish"
        proc._service_subprocess(max_wait_time=0.1)
    proc.close()

    client.variables.get.assert_called_once_with("probe_var")
    [probe_result] = proc.parsing_result.warnings
    assert TaskHandlerParsingResult.model_validate(probe_result) == TaskHandlerParsingResult(
        fileloc=os.fspath(artifact),
        task_handlers={"etl": [TaskHandlerDeclaration(task_id="from-the-dag-processor", params=[])]},
    )
