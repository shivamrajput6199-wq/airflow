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
"""
Route Dag files by the bundle's Dag importer registry.

The Dag processor manager discovers ``.py`` files and zip archives itself. A file with an extension
that another importer in the bundle's registry handles, such as a JAR, is queued when that importer
lists it. A file whose importer is a coordinator's Dag importer is parsed by that coordinator's runtime.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter  # noqa: SDK001
from airflow.sdk.importers import (  # noqa: SDK001
    AbstractDagImporter,
    DagImporterRegistry,
    DagImportError,
    PythonDagImporter,
    ZipImporter,
    find_file_dag_definitions,
    get_file_suffix,
    get_importer_registry,
)
from airflow.utils.file import find_enclosing_file

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.sdk.coordinators._subprocess import SubprocessCoordinator  # noqa: SDK001

log = logging.getLogger(__name__)


def _group_other_importers(
    registry: DagImporterRegistry,
) -> list[tuple[AbstractDagImporter[Any] | None, list[str]]]:
    """
    Pair each importer that is not a Python or zip importer with its extensions.

    An importer that cannot be loaded is ``None``, with the extensions it was configured for.
    """
    groups: list[tuple[AbstractDagImporter[Any] | None, list[str]]] = []
    for ext in registry.supported_extensions():
        try:
            # Any file name works: the registry routes by its suffix.
            importer = registry.get_importer(f"_{ext}")
        except Exception:
            log.exception("Cannot load the Dag importer for %s files", ext)
            importer = None
        if isinstance(importer, (PythonDagImporter, ZipImporter)):
            continue
        group = next((g for g in groups if importer is not None and g[0] is importer), None)
        if group is None:
            groups.append((importer, [ext]))
        else:
            group[1].append(ext)
    return groups


def _get_registry(bundle_name: str | None) -> DagImporterRegistry | None:
    try:
        return get_importer_registry(bundle_name)
    except Exception:
        log.exception("Cannot build the Dag importer registry for bundle %s", bundle_name)
        return None


def is_other_importer_file(bundle_name: str, path: str | os.PathLike[str]) -> bool:
    """Return whether a non-Python importer of the bundle handles ``path``, judged by its extension."""
    if (registry := _get_registry(bundle_name)) is None:
        return False
    suffix = get_file_suffix(Path(path))
    return any(suffix in extensions for _, extensions in _group_other_importers(registry))


def get_claiming_coordinator(
    path: str | os.PathLike[str], bundle_name: str | None
) -> SubprocessCoordinator | None:
    """
    Return the coordinator whose runtime parses ``path``, or ``None`` when a Python child parses it.

    A runtime parses the file when its importer is a coordinator's Dag importer.
    """
    if (registry := _get_registry(bundle_name)) is None:
        return None
    try:
        importer = registry.get_importer(Path(path))
    except Exception:
        log.exception("Cannot load the Dag importer for %s", path)
        return None
    return importer.coordinator if isinstance(importer, CoordinatorDagImporter) else None


def _get_listed_file(item: object, bundle_path: Path) -> Path | None:
    """Return the bundle file that a listed definition or discovery error names, or ``None``."""
    reference = item.source_reference if isinstance(item, DagImportError) else repr(item)
    if (path := find_enclosing_file(bundle_path / reference)) is None:
        return None
    if not path.resolve().is_relative_to(bundle_path.resolve()):
        return None
    return path


def merge_other_importer_files(bundle: BaseDagBundle, file_paths: list[str], *, safe_mode: bool) -> list[str]:
    """
    Replace the files of non-Python importers in ``file_paths`` with the files those importers list.

    A file named by a discovery error is kept, so that parsing it records the error. When an importer
    cannot be loaded, or raises while listing, every file with its extensions is kept: parsing each one
    then reports the failure, instead of the files' Dags being treated as deleted.
    """
    if (registry := _get_registry(bundle.name)) is None:
        return file_paths
    groups = _group_other_importers(registry)
    if not groups:
        return file_paths

    bundle_path = Path(bundle.path)
    other_extensions = {ext for _, extensions in groups for ext in extensions}
    listed: dict[str, None] = {}
    for importer, extensions in groups:
        if importer is not None:
            try:
                for item in importer.list_dag_definitions(bundle, safe_mode=safe_mode):
                    if isinstance(item, DagImportError):
                        log.warning("Dag discovery error: %s", item.format_message())
                    if (path := _get_listed_file(item, bundle_path)) is not None:
                        listed.setdefault(os.fspath(path))
                continue
            except Exception:
                log.exception("Cannot list the Dag files of %s", type(importer).__name__)
        for definition in find_file_dag_definitions(bundle_path, extensions):
            listed.setdefault(os.fspath(definition.path))
    kept = [path for path in file_paths if get_file_suffix(Path(path)) not in other_extensions]
    return kept + [path for path in listed if path not in kept]
