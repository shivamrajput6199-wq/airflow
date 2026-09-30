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
"""Ask the Lang-SDK runtime of a coordinator which task handlers an artifact registers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import attrs

from airflow.dag_processing.lang_sdk_runtime import (
    BaseLangSDKRuntimeProcess,
    StartLangSDKRuntime,
    exec_lang_sdk_runtime,
)
from airflow.dag_processing.processor import TaskHandlerParseRequest, TaskHandlerParsingResult
from airflow.sdk.execution_time.coordinator import get_coordinator_manager
from airflow.sdk.execution_time.supervisor import register_request_method

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from pydantic import BaseModel
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.execution_time.supervisor import RequestHandler
    from airflow.typing_compat import Self


class StartTaskHandlerRuntime(StartLangSDKRuntime):
    """Ask the parse child to exec the runtime of the coordinator configured under *coordinator*."""

    coordinator: str
    """The coordinator's key in ``[sdk] coordinators``."""
    type: Literal["StartTaskHandlerRuntime"] = "StartTaskHandlerRuntime"


def _launch_task_handler_runtime(
    msg: StartTaskHandlerRuntime, report_schema_version: Callable[[str | None], None]
) -> None:
    get_coordinator_manager().get_coordinator(msg.coordinator).parse_task_handler(
        path=Path(msg.file),
        bundle_path=msg.bundle_path,
        comm_address=msg.comm_address,
        logs_address=msg.logs_address,
        report_schema_version=report_schema_version,
    )


def _parse_task_handler_entrypoint() -> None:
    exec_lang_sdk_runtime(StartTaskHandlerRuntime, _launch_task_handler_runtime)


@attrs.define(kw_only=True)
class SDKTaskHandlerProcessorProcess(
    BaseLangSDKRuntimeProcess[TaskHandlerParseRequest, TaskHandlerParsingResult]
):
    """
    Ask a coordinator's runtime which task handlers an artifact registers for some Dags.

    It runs in a Dag-parsing child, which has no API client, so the runtime's requests are relayed to
    the Dag processor. A failure is an import error on the result, keyed by the artifact's path in its
    Dag bundle. On Linux the runtime is killed when the thread that started this process exits, so
    start it from a thread that outlives the parse.
    """

    coordinator: str
    """The coordinator's key in ``[sdk] coordinators``."""

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        coordinator: str,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        artifact_rel_path: str,
        dag_ids: Iterable[str],
        **kwargs,
    ) -> Self:
        """
        Start probing the artifact at *path* for the handlers it registers for *dag_ids*.

        *bundle_path* and *bundle_name* are those of the Dag bundle holding the artifact, and
        *artifact_rel_path* is the artifact's path in it.
        """
        return super().start(
            target=_parse_task_handler_entrypoint,
            parse_request=TaskHandlerParseRequest(
                file=os.fspath(path),
                dag_ids=list(dag_ids),
                bundle_path=bundle_path,
                bundle_name=bundle_name,
            ),
            coordinator=coordinator,
            bundle_name=bundle_name,
            dag_file_rel_path=artifact_rel_path,
            **kwargs,
        )

    @classmethod
    def run(
        cls,
        *,
        coordinator: str,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        artifact_rel_path: str,
        dag_ids: Iterable[str],
        logger: FilteringBoundLogger,
    ) -> TaskHandlerParsingResult:
        """
        Probe the artifact at *path* as :meth:`start` does, and wait for the result.

        The artifact's ``get_dagbag_import_timeout`` bounds the probe, and
        ``[dag_processor] dag_file_processor_timeout`` until the parse child resolves it.
        """
        return cls._run_to_completion(
            coordinator=coordinator,
            path=path,
            bundle_path=bundle_path,
            bundle_name=bundle_name,
            artifact_rel_path=artifact_rel_path,
            dag_ids=dag_ids,
            logger=logger,
        )

    def _build_start_request(
        self, *, comm_address: tuple[str, int], logs_address: tuple[str, int]
    ) -> StartTaskHandlerRuntime:
        return StartTaskHandlerRuntime(
            file=self._parse_request.file,
            bundle_path=self._parse_request.bundle_path,
            coordinator=self.coordinator,
            comm_address=comm_address,
            logs_address=logs_address,
        )

    def _build_import_error_result(self, message: str) -> TaskHandlerParsingResult:
        return TaskHandlerParsingResult(
            fileloc=self._parse_request.file,
            task_handlers={},
            import_errors={self.dag_file_rel_path: message},
        )

    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[Any]]] = {
        **BaseLangSDKRuntimeProcess._request_handlers,
        **dict(
            [
                register_request_method(
                    TaskHandlerParsingResult, BaseLangSDKRuntimeProcess._handle_parsing_result
                )
            ]
        ),
    }
