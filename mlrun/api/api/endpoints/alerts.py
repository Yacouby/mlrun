# Copyright 2023 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import typing
from http import HTTPStatus

from fastapi import APIRouter, Depends, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

import mlrun.common.schemas
from mlrun.api.api import deps
from mlrun.utils import logger

router = APIRouter(prefix="/projects/{project}/alerts")


@router.post("/{name}", response_model=mlrun.common.schemas.AlertConfig)
async def create_alert(
    request: Request,
    project: str,
    name: str,
    alert_data: mlrun.common.schemas.AlertConfig,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
) -> mlrun.common.schemas.AlertConfig:
    await run_in_threadpool(
        mlrun.api.utils.singletons.project_member.get_project_member().ensure_project,
        db_session,
        project,
        auth_info=auth_info,
    )
    await mlrun.api.utils.auth.verifier.AuthVerifier().query_project_permissions(
        project,
        mlrun.common.schemas.AuthorizationAction.create,
        auth_info,
    )

    if (
        mlrun.mlconf.httpdb.clusterization.role
        != mlrun.common.schemas.ClusterizationRole.chief
    ):
        chief_client = mlrun.api.utils.clients.chief.Client()
        data = await request.json()
        return await chief_client.create_alert(
            project=project, name=name, request=request, json=data
        )

    logger.debug(f"Creating alert {name}", project=project)

    return await run_in_threadpool(
        mlrun.api.crud.Alerts().create_alert,
        db_session,
        name,
        project,
        alert_data,
    )


@router.put("/{alert_id}", response_model=mlrun.common.schemas.AlertConfig)
async def store_alert(
    request: Request,
    project: str,
    alert_id: str,
    alert_data: mlrun.common.schemas.AlertConfig,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
) -> mlrun.common.schemas.AlertConfig:
    await run_in_threadpool(
        mlrun.api.utils.singletons.project_member.get_project_member().ensure_project,
        db_session,
        project,
        auth_info=auth_info,
    )
    await mlrun.api.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
        mlrun.common.schemas.AuthorizationResourceTypes.alert,
        project,
        alert_id,
        mlrun.common.schemas.AuthorizationAction.create,
        auth_info,
    )

    if (
        mlrun.mlconf.httpdb.clusterization.role
        != mlrun.common.schemas.ClusterizationRole.chief
    ):
        chief_client = mlrun.api.utils.clients.chief.Client()
        data = await request.json()
        return await chief_client.store_alert(
            project=project, alert_id=alert_id, request=request, json=data
        )

    logger.debug(f"Updating alert {alert_id}", project=project)

    return await run_in_threadpool(
        mlrun.api.crud.Alerts().store_alert,
        db_session,
        project,
        alert_id,
        alert_data,
    )


@router.get(
    "/{alert_id}",
    response_model=mlrun.common.schemas.AlertConfig,
)
async def get_alert(
    project: str,
    alert_id: str,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
) -> mlrun.common.schemas.AlertConfig:
    await run_in_threadpool(
        mlrun.api.utils.singletons.project_member.get_project_member().ensure_project,
        db_session,
        project,
        auth_info=auth_info,
    )

    await mlrun.api.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
        mlrun.common.schemas.AuthorizationResourceTypes.alert,
        project,
        alert_id,
        mlrun.common.schemas.AuthorizationAction.read,
        auth_info,
    )

    resp = await run_in_threadpool(
        mlrun.api.crud.Alerts().get_enriched_alert_by_id, db_session, alert_id
    )

    if resp is None:
        raise mlrun.errors.MLRunNotFoundError(
            f"Alert {alert_id} for project {project} not found"
        )

    return resp


@router.get("", response_model=typing.List[mlrun.common.schemas.AlertConfig])
async def list_alerts(
    project: str,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
) -> typing.List[mlrun.common.schemas.AlertConfig]:
    await run_in_threadpool(
        mlrun.api.utils.singletons.project_member.get_project_member().ensure_project,
        db_session,
        project,
        auth_info=auth_info,
    )
    await mlrun.api.utils.auth.verifier.AuthVerifier().query_project_permissions(
        project,
        mlrun.common.schemas.AuthorizationAction.read,
        auth_info,
    )

    return await run_in_threadpool(
        mlrun.api.crud.Alerts().list_alerts, db_session, project
    )


@router.delete(
    "/{alert_id}",
    status_code=HTTPStatus.NO_CONTENT.value,
)
async def delete_alert(
    request: Request,
    project: str,
    alert_id: str,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
):
    await run_in_threadpool(
        mlrun.api.utils.singletons.project_member.get_project_member().ensure_project,
        db_session,
        project,
        auth_info=auth_info,
    )

    await mlrun.api.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
        mlrun.common.schemas.AuthorizationResourceTypes.alert,
        project,
        alert_id,
        mlrun.common.schemas.AuthorizationAction.delete,
        auth_info,
    )

    if (
        mlrun.mlconf.httpdb.clusterization.role
        != mlrun.common.schemas.ClusterizationRole.chief
    ):
        chief_client = mlrun.api.utils.clients.chief.Client()
        return await chief_client.delete_alert(
            project=project, alert_id=alert_id, request=request
        )

    await run_in_threadpool(
        mlrun.api.crud.Alerts().delete_alert, db_session, project, alert_id
    )


@router.post("/{alert_id}/reset")
async def reset_alert(
    request: Request,
    project: str,
    alert_id: str,
    auth_info: mlrun.common.schemas.AuthInfo = Depends(deps.authenticate_request),
    db_session: Session = Depends(deps.get_db_session),
):
    await run_in_threadpool(
        mlrun.api.utils.singletons.project_member.get_project_member().ensure_project,
        db_session,
        project,
        auth_info=auth_info,
    )
    await mlrun.api.utils.auth.verifier.AuthVerifier().query_project_resource_permissions(
        mlrun.common.schemas.AuthorizationResourceTypes.alert,
        project,
        alert_id,
        mlrun.common.schemas.AuthorizationAction.update,
        auth_info,
    )

    if (
        mlrun.mlconf.httpdb.clusterization.role
        != mlrun.common.schemas.ClusterizationRole.chief
    ):
        chief_client = mlrun.api.utils.clients.chief.Client()
        return await chief_client.reset_alert(
            project=project, alert_id=alert_id, request=request
        )

    logger.debug(f"Resetting alert {alert_id}", project=project)

    return await run_in_threadpool(
        mlrun.api.crud.Alerts().reset_alert, db_session, project, alert_id
    )
