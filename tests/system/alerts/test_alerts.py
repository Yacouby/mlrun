# Copyright 2024 Iguazio
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
import time
import typing

import pytest
import requests

import mlrun
import mlrun.common.schemas.alert as alert_constants
import mlrun.model_monitoring.api
from tests.system.base import TestMLRunSystem


@TestMLRunSystem.skip_test_if_env_not_configured
class TestAlerts(TestMLRunSystem):
    project_name = "alerts-test-project"

    # Set image to "<repo>/mlrun:<tag>" for local testing
    image: typing.Optional[str] = None

    def test_job_failure_alert(self):
        """
        validate that an alert is sent in case a job fails
        """
        self.project.set_function(
            name="test-func",
            func="assets/function.py",
            handler="handler",
            image="mlrun/mlrun" if self.image is None else self.image,
            kind="job",
        )

        # nuclio function for storing notifications, to validate that alert notifications were sent on the failed job
        nuclio_function = self.project.set_function(
            name="nuclio",
            func="../assets/notification_nuclio_function.py",
            image="mlrun/mlrun" if self.image is None else self.image,
            kind="nuclio",
        )
        nuclio_function.deploy()
        nuclio_function_url = nuclio_function.spec.command

        # create an alert with webhook notification
        alert_name = "failure_webhook"
        alert_summary = "Job failed"
        self._create_alert_config(
            self.project_name,
            alert_name,
            alert_constants.EventEntityKind.JOB,
            alert_summary,
            alert_constants.EventKind.FAILED,
            nuclio_function_url,
        )

        with pytest.raises(Exception):
            self.project.run_function("test-func")

        # in order to trigger the periodic monitor runs function, to detect the failed run and send an event on it
        time.sleep(35)

        # Validate that the notifications was sent on the failed job
        self._validate_notifications_on_nuclio(nuclio_function_url)

    def _create_alert_config(
        self,
        project,
        name,
        entity_kind,
        summary,
        event_name,
        nuclio_function_url,
        criteria=None,
    ):
        notifications = [
            {
                "kind": "webhook",
                "name": "failure",
                "message": "job failed !",
                "severity": "warning",
                "when": ["now"],
                "condition": "failed",
                "params": {
                    "url": nuclio_function_url,
                    "override_body": {
                        "operation": "add",
                        "data": "notification failure",
                    },
                },
                "secret_params": {
                    "webhook": "some-webhook",
                },
            },
        ]
        alert_data = mlrun.common.schemas.AlertConfig(
            project=project,
            name=name,
            summary=summary,
            severity="low",
            entity={"kind": entity_kind, "project": project, "id": "*"},
            trigger={"events": [event_name]},
            criteria=criteria,
            notifications=notifications,
        ).dict()

        mlrun.get_run_db().store_alert_config(name, alert_data)

    def _validate_notifications_on_nuclio(self, nuclio_function_url):
        expected_element = "notification failure"

        response = requests.post(nuclio_function_url, json={"operation": "get"})
        response_data = json.loads(response.text)
        assert response_data["element"] == expected_element

        requests.post(nuclio_function_url, json={"operation": "reset"})
