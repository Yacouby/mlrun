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
import json
from http import HTTPStatus

import pytest

import mlrun
import mlrun.common.schemas
import mlrun.utils
import tests.integration.sdk_api.base


def _generate_event_request(project, event_kind, entity_kind):
    request = mlrun.common.schemas.Event(
        kind=event_kind,
        entity={"kind": entity_kind, "project": project, "id": 1234},
        value=0.2,
    ).dict()
    return mlrun.utils.dict_to_json(request)


def _generate_alert_create_request(
    project, name, entity_kind, summary, event_name, criteria=None, notifications=None
):
    if notifications is None:
        notifications = [
            {
                "kind": "slack",
                "name": "slack_drift",
                "message": "Ay caramba!",
                "severity": "warning",
                "when": ["now"],
                "secret_params": {
                    "webhook": "https://hooks.slack.com/services/",
                },
                "condition": "oops",
            },
        ]
    request = mlrun.common.schemas.AlertConfig(
        project=project,
        name=name,
        summary=summary,
        severity="low",
        entity={"kind": entity_kind, "project": project, "id": "*"},
        trigger={"events": [event_name]},
        criteria=criteria,
        notifications=notifications,
    ).dict()
    return mlrun.utils.dict_to_json(request)


class TestAlerts(tests.integration.sdk_api.base.TestMLRunIntegration):
    def test_basic_alerts(self):
        project_name = "my-project"

        event_name = "drift_detected"
        alert_name = "drift"
        alert_summary = "Model {{ $project }}/{{ $entity }} is drifting."
        alert_entity = "model"

        alert2_name = "jobs"
        alert2_entity = "job"
        alert2_summary = "Job {{ $project }}/{{ $entity }} failed."
        event2_name = "failed"
        alert2_count = 3

        mlrun.new_project(project_name)

        # validate get alerts on empty system
        response = mlrun.get_run_db().api_call("GET", f"projects/{project_name}/alerts")
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert len(resp_dict) == 0

        # post unhandled event
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/events/{event_name}",
            body=_generate_event_request(project_name, event_name, alert_entity),
        )
        assert response.status_code == HTTPStatus.OK.value

        # create alert with non-existent project
        with pytest.raises(mlrun.errors.MLRunNotFoundError):
            mlrun.get_run_db().api_call(
                "POST",
                f"projects/no_such_project/alerts/{alert_name}",
                body=_generate_alert_create_request(
                    project_name, alert_name, alert_entity, alert_summary, event_name
                ),
            )

        # create alert
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/alerts/{alert_name}",
            body=_generate_alert_create_request(
                project_name, alert_name, alert_entity, alert_summary, event_name
            ),
        )
        assert response.status_code == HTTPStatus.OK.value
        created_alert = resp_dict = json.loads(response.text)
        assert resp_dict["id"] >= 1
        assert resp_dict["project"] == project_name
        assert resp_dict["name"] == alert_name
        assert resp_dict["summary"] == alert_summary
        assert resp_dict["state"] == "inactive"

        # try to create same alert again
        with pytest.raises(mlrun.errors.MLRunConflictError):
            mlrun.get_run_db().api_call(
                "POST",
                f"projects/{project_name}/alerts/{alert_name}",
                body=_generate_alert_create_request(
                    project_name, alert_name, alert_entity, alert_summary, event_name
                ),
            )

        notifications = [
            {
                "kind": "slack",
                "name": "slack_jobs",
                "message": "Ay ay ay!",
                "severity": "warning",
                "when": ["now"],
                "condition": "failed",
                "secret_params": {
                    "webhook": "https://hooks.slack.com/services/",
                },
            },
            {
                "kind": "git",
                "name": "git_jobs",
                "message": "Ay ay ay!",
                "severity": "warning",
                "when": ["now"],
                "condition": "failed",
                "secret_params": {
                    "webhook": "https://hooks.slack.com/services/",
                },
            },
        ]

        # create another alert
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/alerts/{alert2_name}",
            body=_generate_alert_create_request(
                project_name,
                alert2_name,
                alert2_entity,
                alert2_summary,
                event2_name,
                criteria={"period": "1h", "count": alert2_count},
                notifications=notifications,
            ),
        )
        assert response.status_code == HTTPStatus.OK.value
        created_alert2 = resp_dict = json.loads(response.text)
        assert resp_dict["id"] >= 1
        assert resp_dict["project"] == project_name
        assert resp_dict["name"] == alert2_name
        assert resp_dict["summary"] == alert2_summary

        response = mlrun.get_run_db().api_call("GET", f"projects/{project_name}/alerts")
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert len(resp_dict) == 2
        assert resp_dict[0]["project"] == project_name
        assert resp_dict[0]["name"] == alert_name
        assert resp_dict[1]["project"] == project_name
        assert resp_dict[1]["name"] == alert2_name

        # get alert and validate params
        response = mlrun.get_run_db().api_call(
            "GET", f"projects/{project_name}/alerts/{created_alert['id']}"
        )
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert resp_dict["project"] == project_name
        assert resp_dict["name"] == alert_name

        # try to get non existent alert ID
        with pytest.raises(mlrun.errors.MLRunNotFoundError):
            mlrun.get_run_db().api_call("GET", f"projects/{project_name}/alerts/666")

        # post event for alert 1
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/events/{event_name}",
            body=_generate_event_request(project_name, event_name, alert_entity),
        )
        assert response.status_code == HTTPStatus.OK.value

        # post event for alert 2
        for _ in range(alert2_count):
            response = mlrun.get_run_db().api_call(
                "POST",
                f"projects/{project_name}/events/{event2_name}",
                body=_generate_event_request(project_name, event2_name, alert2_entity),
            )
            assert response.status_code == HTTPStatus.OK.value

        # post event with invalid entity type
        with pytest.raises(mlrun.errors.MLRunBadRequestError):
            mlrun.get_run_db().api_call(
                "POST",
                f"projects/{project_name}/events/{event_name}",
                body=_generate_event_request(project_name, event_name, "job"),
            )

        # modify alert
        new_summary = "Aye ya yay {{ $project }}"
        new_event_name = "drift_suspected"
        response = mlrun.get_run_db().api_call(
            "PUT",
            f"projects/{project_name}/alerts/{created_alert['id']}",
            body=_generate_alert_create_request(
                project_name, alert_name, alert_entity, new_summary, new_event_name
            ),
        )
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)

        new_event_id = resp_dict["id"]
        # verify that modify alert succeeded
        response = mlrun.get_run_db().api_call(
            "GET", f"projects/{project_name}/alerts/{new_event_id}"
        )
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert resp_dict["project"] == project_name
        assert resp_dict["name"] == alert_name
        assert resp_dict["id"] == created_alert["id"]
        assert resp_dict["summary"] == new_summary
        assert resp_dict["trigger"]["events"] == [new_event_name]
        assert resp_dict["state"] == "inactive"

        # post new event to make sure the alert handles it
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/events/{new_event_name}",
            body=_generate_event_request(project_name, new_event_name, "model"),
        )
        assert response.status_code == HTTPStatus.OK.value

        response = mlrun.get_run_db().api_call(
            "GET", f"projects/{project_name}/alerts/{new_event_id}"
        )
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert resp_dict["state"] == "active"

        # reset alert
        response = mlrun.get_run_db().api_call(
            "POST", f"projects/{project_name}/alerts/{created_alert['id']}/reset"
        )
        assert response.status_code == HTTPStatus.OK.value

        response = mlrun.get_run_db().api_call(
            "GET", f"projects/{project_name}/alerts/{new_event_id}"
        )
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert resp_dict["state"] == "inactive"

        # delete alert
        response = mlrun.get_run_db().api_call(
            "DELETE", f"projects/{project_name}/alerts/{created_alert['id']}"
        )
        assert response.status_code == HTTPStatus.NO_CONTENT.value

        response = mlrun.get_run_db().api_call("GET", f"projects/{project_name}/alerts")
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert len(resp_dict) == 1

        # try to delete invalid alert
        response = mlrun.get_run_db().api_call(
            "DELETE", f"projects/{project_name}/alerts/666"
        )
        assert response.status_code == HTTPStatus.NO_CONTENT.value

        response = mlrun.get_run_db().api_call(
            "DELETE", f"projects/{project_name}/alerts/{created_alert2['id']}"
        )
        assert response.status_code == HTTPStatus.NO_CONTENT.value

        # validate get alerts on empty system after deletes
        response = mlrun.get_run_db().api_call("GET", f"projects/{project_name}/alerts")
        assert response.status_code == HTTPStatus.OK.value
        resp_dict = json.loads(response.text)
        assert len(resp_dict) == 0

        mlrun.get_run_db().delete_project(project_name)

    def test_basic_alerts2(self):
        project_name = "my-new-project"
        event_name = "drift_detected"
        alert_name = "drift"
        alert_summary = "Model {{ $project }}/{{ $entity }} is drifting."
        alert_entity = "model"

        mlrun.new_project(project_name)

        notifications = [
            {
                "kind": "slack",
                "name": "",
                "message": "Ay caramba!",
                "severity": "warning",
                "when": ["now"],
                "condition": "oops",
                "secret_params": {
                    "webhook": "https://hooks.slack.com/services/",
                },
            },
        ]

        # create alert
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/alerts/{alert_name}",
            body=_generate_alert_create_request(
                project_name,
                alert_name,
                alert_entity,
                alert_summary,
                event_name,
                notifications=notifications,
            ),
        )
        assert response.status_code == HTTPStatus.OK.value

        # post event
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/events/{event_name}",
            body=_generate_event_request(project_name, event_name, alert_entity),
        )
        assert response.status_code == HTTPStatus.OK.value

        mlrun.get_run_db().delete_project(project_name, "cascade")

        mlrun.new_project(project_name)

        # create alert
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/alerts/{alert_name}",
            body=_generate_alert_create_request(
                project_name,
                alert_name,
                alert_entity,
                alert_summary,
                event_name,
                notifications=notifications,
            ),
        )
        assert response.status_code == HTTPStatus.OK.value

        # return

        # post event
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/events/{event_name}",
            body=_generate_event_request(project_name, event_name, alert_entity),
        )
        assert response.status_code == HTTPStatus.OK.value
