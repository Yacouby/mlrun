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

import pydantic.error_wrappers
import pytest

import mlrun
import mlrun.common.schemas
import mlrun.utils
import tests.integration.sdk_api.base


class TestAlerts(tests.integration.sdk_api.base.TestMLRunIntegration):
    def test_basic_alerts(self):
        project_name = "my-project"

        # Define parameters for alert 1
        alert1 = {
            "name": "drift",
            "entity": {"kind": "model", "project": project_name},
            "summary": "Model {{ $project }}/{{ $entity }} is drifting.",
            "event_name": "drift_detected",
            "state": "inactive",
        }

        # Define parameters for alert 2
        alert2 = {
            "name": "jobs",
            "entity": {"kind": "job", "project": project_name},
            "summary": "Job {{ $project }}/{{ $entity }} failed.",
            "event_name": "failed",
            "state": "inactive",
            "count": 3,
        }

        mlrun.new_project(project_name)

        # validate get alerts on empty system
        alerts = self._get_alerts(project_name)
        assert len(alerts) == 0

        # validate create alert operation
        created_alert, created_alert2 = self._create_alerts_test(
            project_name, alert1, alert2
        )

        # validate get alerts on the created alerts
        alerts = self._get_alerts(project_name)
        assert len(alerts) == 2
        self._validate_alert(alerts[0], project_name, alert1["name"])
        self._validate_alert(alerts[1], project_name, alert2["name"])

        # get alert and validate params
        alert = self._get_alerts(project_name, created_alert["id"])
        self._validate_alert(alert, project_name, alert1["name"])

        # try to get non existent alert ID
        with pytest.raises(mlrun.errors.MLRunNotFoundError):
            self._get_alerts(project_name, alert_id=666)

        # post event with invalid entity type
        with pytest.raises(mlrun.errors.MLRunBadRequestError):
            self._post_event(
                project_name, alert1["event_name"], alert2["entity"]["kind"]
            )

        # post event for alert 1
        self._post_event(project_name, alert1["event_name"], alert1["entity"]["kind"])

        # post event for alert 2
        for _ in range(alert2["count"]):
            self._post_event(
                project_name, alert2["event_name"], alert2["entity"]["kind"]
            )

        # since the reset_policy of the alert is "auto", the state now should be inactive
        alert = self._get_alerts(project_name, created_alert2["id"])
        self._validate_alert(alert, alert_state="inactive")

        new_event_name = "drift_suspected"
        modified_alert = self._modify_alert_test(
            project_name, alert1, created_alert["id"], new_event_name
        )

        # post new event to make sure the modified alert handles it
        self._post_event(project_name, new_event_name, alert1["entity"]["kind"])

        alert = self._get_alerts(project_name, modified_alert["id"])
        self._validate_alert(alert, alert_state="active")

        # reset alert
        self._reset_alert(project_name, created_alert["id"])

        alert = self._get_alerts(project_name, created_alert["id"])
        self._validate_alert(alert, alert_state="inactive")

        # reset the alert again, and validate that the state is still inactive
        self._reset_alert(project_name, created_alert["id"])

        alert = self._get_alerts(project_name, created_alert["id"])
        self._validate_alert(alert, alert_state="inactive")

        # delete alert
        self._delete_alert(project_name, created_alert["id"])

        alerts = self._get_alerts(project_name)
        assert len(alerts) == 1

        # try to delete invalid alert
        self._delete_alert(project_name, alert_id=666)

        self._delete_alert(project_name, created_alert2["id"])

        # validate get alerts on empty system after deletes
        alerts = self._get_alerts(project_name)
        assert len(alerts) == 0

        mlrun.get_run_db().delete_project(project_name)

    def test_basic_alerts2(self):
        # this test checks create alert and post event operations after deleting a project and creating it again
        # with the same alert and event names

        project_name = "my-new-project"
        event_name = "drift_detected"
        alert_name = "drift"
        alert_summary = "Model {{ $project }}/{{ $entity }} is drifting."
        alert_entity_kind = "model"
        alert_entity_project = project_name

        mlrun.new_project(project_name)

        # create alert
        self._create_alert(
            project_name,
            alert_name,
            alert_entity_kind,
            alert_entity_project,
            alert_summary,
            event_name,
        )

        # post event on the created alert
        self._post_event(project_name, event_name, alert_entity_kind)

        # delete the project
        mlrun.get_run_db().delete_project(project_name, "cascade")

        # create a new project with the same name
        mlrun.new_project(project_name)

        # create alert
        self._create_alert(
            project_name,
            alert_name,
            alert_entity_kind,
            alert_entity_project,
            alert_summary,
            event_name,
        )

        # post event
        self._post_event(project_name, event_name, alert_entity_kind)

    def _create_alerts_test(self, project_name, alert1, alert2):
        # create alert with non-existent project
        invalid_project = "no_such_project"
        with pytest.raises(mlrun.errors.MLRunNotFoundError):
            self._create_alert(
                invalid_project,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
            )

        # create alert with invalid entity kind
        invalid_entity_kind = "endpoint"
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            self._create_alert(
                project_name,
                alert1["name"],
                invalid_entity_kind,
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
            )

        """ TODO: uncomment after handled
        # create alert with invalid entity project
        invalid_entity_project = "no_such_project"
        with pytest.raises(mlrun.errors.MLRunNotFoundError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                invalid_entity_project,
                alert1["summary"],
                alert1["event_name"],
            )
        """

        # create alert with invalid severity
        invalid_severity = "critical"
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
                severity=invalid_severity,
            )

        # create alert with invalid criteria period
        invalid_criteria = {"period": "1"}  # for example, it should be "1h"
        with pytest.raises(mlrun.errors.MLRunBadRequestError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
                criteria=invalid_criteria,
            )

        # create alert with invalid reset policy
        invalid_policy = "scheduled"
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
                reset_policy=invalid_policy,
            )

        # create alert with invalid notification kind
        invalid_notification = [
            {
                "kind": "invalid",
                "name": "invalid_notification",
                "message": "Ay ay ay!",
                "severity": "warning",
                "when": ["now"],
                "condition": "failed",
                "secret_params": {
                    "webhook": "https://hooks.slack.com/services/",
                },
            },
        ]
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
                notifications=invalid_notification,
            )

        # create alert with two notifications with the same name - should fail
        duplicated_names_notifications = [
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
                "name": "slack_jobs",
                "message": "Ay ay ay!",
                "severity": "warning",
                "when": ["now"],
                "condition": "failed",
                "secret_params": {
                    "webhook": "https://hooks.slack.com/services/",
                },
            },
        ]
        with pytest.raises(mlrun.errors.MLRunBadRequestError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
                notifications=duplicated_names_notifications,
            )

        # create alert with no errors
        created_alert = self._create_alert(
            project_name,
            alert1["name"],
            alert1["entity"]["kind"],
            alert1["entity"]["project"],
            alert1["summary"],
            alert1["event_name"],
        )
        self._validate_alert(
            created_alert,
            project_name,
            alert1["name"],
            alert1["summary"],
            alert1["state"],
            alert1["event_name"],
        )

        # try to create same alert again
        with pytest.raises(mlrun.errors.MLRunConflictError):
            self._create_alert(
                project_name,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
            )

        # create another alert
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

        created_alert2 = self._create_alert(
            project_name,
            alert2["name"],
            alert2["entity"]["kind"],
            alert2["entity"]["project"],
            alert2["summary"],
            alert2["event_name"],
            criteria={"period": "1h", "count": alert2["count"]},
            reset_policy="auto",
            notifications=notifications,
        )
        self._validate_alert(
            created_alert2,
            project_name,
            alert2["name"],
            alert2["summary"],
            alert2["state"],
            alert2["event_name"],
        )

        return created_alert, created_alert2

    def _modify_alert_test(self, project_name, alert1, alert_id, new_event_name):
        # modify alert name - should fail as changing alert name is not allowed
        new_alert_name = "drift_modify"
        with pytest.raises(mlrun.errors.MLRunBadRequestError):
            self._modify_alert(
                project_name,
                alert_id,
                new_alert_name,
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                alert1["event_name"],
            )

        # modify alert with invalid data
        invalid_event_name = "not_permitted_event"
        with pytest.raises(pydantic.error_wrappers.ValidationError):
            self._modify_alert(
                project_name,
                alert_id,
                alert1["name"],
                alert1["entity"]["kind"],
                alert1["entity"]["project"],
                alert1["summary"],
                invalid_event_name,
            )

        # modify alert with no errors
        new_summary = "Aye ya yay {{ $project }}"
        modified_alert = self._modify_alert(
            project_name,
            alert_id,
            alert1["name"],
            alert1["entity"]["kind"],
            alert1["entity"]["project"],
            new_summary,
            new_event_name,
        )

        new_event_id = modified_alert["id"]
        # verify that modify alert succeeded
        alert = self._get_alerts(project_name, new_event_id)
        self._validate_alert(
            alert,
            project_name,
            alert1["name"],
            new_summary,
            alert1["state"],
            new_event_name,
            alert_id,
        )

        return modified_alert

    def _create_alert(
        self,
        project_name,
        alert_name,
        alert_entity_kind,
        alert_entity_project,
        alert_summary,
        event_name,
        severity="low",
        criteria=None,
        notifications=None,
        reset_policy="manual",
    ):
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/alerts/{alert_name}",
            body=self._generate_alert_create_request(
                project_name,
                alert_name,
                alert_entity_kind,
                alert_entity_project,
                alert_summary,
                event_name,
                severity,
                criteria,
                notifications,
                reset_policy,
            ),
        )
        assert response.status_code == HTTPStatus.OK.value
        return json.loads(response.text)

    def _modify_alert(
        self,
        project_name,
        alert_id,
        alert_name,
        alert_entity_kind,
        alert_entity_project,
        alert_summary,
        event_name,
        severity="low",
        criteria=None,
        notifications=None,
        reset_policy="manual",
    ):
        response = mlrun.get_run_db().api_call(
            "PUT",
            f"projects/{project_name}/alerts/{alert_id}",
            body=self._generate_alert_create_request(
                project_name,
                alert_name,
                alert_entity_kind,
                alert_entity_project,
                alert_summary,
                event_name,
                severity,
                criteria,
                notifications,
                reset_policy,
            ),
        )
        assert response.status_code == HTTPStatus.OK.value
        return json.loads(response.text)

    def _post_event(self, project_name, event_name, alert_entity_kind):
        response = mlrun.get_run_db().api_call(
            "POST",
            f"projects/{project_name}/events/{event_name}",
            body=self._generate_event_request(
                project_name, event_name, alert_entity_kind
            ),
        )
        assert response.status_code == HTTPStatus.OK.value

    @staticmethod
    def _get_alerts(project_name, alert_id=None):
        if alert_id:
            response = mlrun.get_run_db().api_call(
                "GET", f"projects/{project_name}/alerts/{alert_id}"
            )
        else:
            response = mlrun.get_run_db().api_call(
                "GET", f"projects/{project_name}/alerts"
            )
        assert response.status_code == HTTPStatus.OK.value
        return json.loads(response.text)

    @staticmethod
    def _reset_alert(project_name, alert_id):
        response = mlrun.get_run_db().api_call(
            "POST", f"projects/{project_name}/alerts/{alert_id}/reset"
        )
        assert response.status_code == HTTPStatus.OK.value

    @staticmethod
    def _delete_alert(project_name, alert_id):
        response = mlrun.get_run_db().api_call(
            "DELETE", f"projects/{project_name}/alerts/{alert_id}"
        )
        assert response.status_code == HTTPStatus.NO_CONTENT.value

    @staticmethod
    def _validate_alert(
        alert,
        project_name=None,
        alert_name=None,
        alert_summary=None,
        alert_state=None,
        alert_event_name=None,
        alert_id=None,
    ):
        assert alert["id"] >= 1
        if project_name:
            assert alert["project"] == project_name
        if alert_name:
            assert alert["name"] == alert_name
        if alert_summary:
            assert alert["summary"] == alert_summary
        if alert_state:
            assert alert["state"] == alert_state
        if alert_event_name:
            assert alert["trigger"]["events"] == [alert_event_name]
        if alert_id:
            assert alert["id"] == alert_id

    @staticmethod
    def _generate_event_request(project, event_kind, entity_kind):
        request = mlrun.common.schemas.Event(
            kind=event_kind,
            entity={"kind": entity_kind, "project": project, "id": 1234},
            value=0.2,
        ).dict()
        return mlrun.utils.dict_to_json(request)

    @staticmethod
    def _generate_alert_create_request(
        project,
        name,
        entity_kind,
        entity_project,
        summary,
        event_name,
        severity,
        criteria,
        notifications,
        reset_policy,
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
            severity=severity,
            entity={"kind": entity_kind, "project": entity_project, "id": "*"},
            trigger={"events": [event_name]},
            criteria=criteria,
            notifications=notifications,
            reset_policy=reset_policy,
        ).dict()
        return mlrun.utils.dict_to_json(request)
