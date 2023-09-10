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
import pathlib
from time import sleep

import pytest
import requests

import mlrun.artifacts
import mlrun.common.schemas
import mlrun.errors
from tests.system.base import TestMLRunSystem


@TestMLRunSystem.skip_test_if_env_not_configured
class TestAPIArtifacts(TestMLRunSystem):
    project_name = "db-system-test-project12"

    @pytest.mark.enterprise
    def test_fail_overflowing_artifact(self):
        """
        Test that we fail when trying to (inline) log an artifact that is too big
        This is done to ensure that we don't corrupt the DB while truncating the data
        """
        filename = str(pathlib.Path(__file__).parent / "assets" / "function.py")
        function = mlrun.code_to_function(
            name="test-func",
            project=self.project_name,
            filename=filename,
            handler="log_artifact_test_function",
            kind="job",
            image="docker.io/yacouby1/mlrun-api:1.5.0",
        )
        task = mlrun.new_task()

        # nuclio function for storing notifications, to validate that alert notifications were sent on the failed job
        nuclio_function = mlrun.code_to_function(
            name="nuclio",
            project=self.project_name,
            filename="../assets/notification_nuclio_function.py",
            image="docker.io/yacouby1/mlrun-api:1.5.0",
            kind="nuclio",
        )
        nuclio_function.deploy()
        nuclio_function_url = nuclio_function.spec.command

        # create an alert with webhook notification
        self._generate_alert_create_request(
            self.project_name,
            "fail_webhook5",
            "job",
            "Job failed",
            "failed",
            nuclio_function_url,
        )

        # run artifact field is MEDIUMBLOB which is limited to 16MB by mysql
        # overflow and expect it to fail execution and not allow db to truncate the data
        # to avoid data corruption
        with pytest.raises(mlrun.runtimes.utils.RunError):
            function.run(
                task, params={"body_size": 16 * 1024 * 1024 + 1, "inline": True}
            )

        runs = mlrun.get_run_db().list_runs()
        assert len(runs) == 1, "run should not be created"
        run = runs[0]
        assert run["status"]["state"] == "error", "run should fail"
        assert (
            "Failed committing changes to DB" in run["status"]["error"]
        ), "run should fail with a reason"

        # in order to trigger the periodic monitor runs function, to detect the failed run and send an event on it
        sleep(30)

        # Validate that the notifications was sent on the failed job
        self._validate_notifications_on_nuclio(nuclio_function_url)

    def _generate_alert_create_request(
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
                "name": "webhook_fail",
                "message": "Ay ay ay!",
                "severity": "warning",
                "when": ["now"],
                "condition": "failed",
                "params": {
                    "url": nuclio_function_url,
                    "override_body": {
                        "operation": "add",
                        "data": "notification on failure",
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

        mlrun.get_run_db().create_alert(name, alert_data)

    def _validate_notifications_on_nuclio(self, nuclio_function_url):
        response = requests.post(nuclio_function_url, json={"operation": "get"})

        expected_element = "notification on failure"
        expected_status_code = 200

        response_data = json.loads(response.text)
        print(response.text)
        assert response_data["element"] == expected_element
        assert response_data["status_code"] == expected_status_code

        # delete the  notification
        response = requests.post(nuclio_function_url, json={"operation": "delete"})
        response_data = json.loads(response.text)
        assert (response_data["status_code"]) == 200

        # the nuclio list should be empty now after deleting all the notifications
        response = requests.post(nuclio_function_url, json={"operation": "get"})
        response_data = json.loads(response.text)
        assert (response_data["message"]) == "List is empty"
        assert (response_data["status_code"]) == 400
