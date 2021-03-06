import unittest.mock
from mlrun.api.utils.singletons.k8s import get_k8s
from mlrun.utils import create_logger
from mlrun.runtimes.constants import PodPhases
from kubernetes import client
from datetime import datetime, timezone
from copy import deepcopy
from mlrun.config import config as mlconf
import json
from mlrun.model import new_task
import deepdiff
import pathlib
import sys
from base64 import b64encode
from kubernetes.client import V1EnvVar

logger = create_logger(level="debug", name="test-runtime")


class TestRuntimeBase:
    def setup_method(self, method):
        self.namespace = mlconf.namespace = "test-namespace"
        get_k8s().namespace = self.namespace
        self._logger = logger
        self.project = "test-project"
        self.name = "test-function"
        self.run_uid = "test_run_uid"
        self.image_name = "mlrun/mlrun:latest"
        self.artifact_path = "/tmp"
        self.code_filename = str(self.assets_path / "sample_function.py")

        self._logger.info(
            f"Setting up test {self.__class__.__name__}::{method.__name__}"
        )

        self.custom_setup()

        self._logger.info(
            f"Finished setting up test {self.__class__.__name__}::{method.__name__}"
        )

    @property
    def assets_path(self):
        return (
            pathlib.Path(sys.modules[self.__module__].__file__).absolute().parent
            / "assets"
        )

    def _generate_runtime(self):
        pass

    def custom_setup(self):
        pass

    def _generate_task(self):
        return new_task(
            name=self.name, project=self.project, artifact_path=self.artifact_path
        )

    def _mock_create_namespaced_pod(self):
        def _generate_pod(namespace, pod):
            terminated_container_state = client.V1ContainerStateTerminated(
                finished_at=datetime.now(timezone.utc), exit_code=0
            )
            container_state = client.V1ContainerState(
                terminated=terminated_container_state
            )
            container_status = client.V1ContainerStatus(
                state=container_state,
                image=self.image_name,
                image_id="must-provide-image-id",
                name=self.name,
                ready=True,
                restart_count=0,
            )
            status = client.V1PodStatus(
                phase=PodPhases.succeeded, container_statuses=[container_status]
            )
            response_pod = deepcopy(pod)
            response_pod.status = status
            response_pod.metadata.name = "test-pod"
            response_pod.metadata.namespace = namespace
            return response_pod

        get_k8s().v1api.create_namespaced_pod = unittest.mock.Mock(
            side_effect=_generate_pod
        )

    def _execute_run(self, runtime, **kwargs):
        # Reset the mock, so that when checking is create_pod was called, no leftovers are there (in case running
        # multiple runs in the same test)
        get_k8s().v1api.create_namespaced_pod.reset_mock()

        runtime.run(
            name=self.name,
            project=self.project,
            artifact_path=self.artifact_path,
            **kwargs,
        )

    def _assert_labels(self, labels: dict, expected_class_name):
        expected_labels = {
            "mlrun/class": expected_class_name,
            "mlrun/name": self.name,
            "mlrun/project": self.project,
            "mlrun/tag": "latest",
        }

        for key in expected_labels:
            assert labels[key] == expected_labels[key]

    def _assert_function_config(
        self,
        config,
        expected_params,
        expected_inputs,
        expected_hyper_params,
        expected_secrets,
    ):
        function_metadata = config["metadata"]
        assert function_metadata["name"] == self.name
        assert function_metadata["project"] == self.project

        function_spec = config["spec"]
        assert function_spec["output_path"] == self.artifact_path
        if expected_params:
            assert (
                deepdiff.DeepDiff(
                    function_spec["parameters"], expected_params, ignore_order=True
                )
                == {}
            )
        if expected_inputs:
            assert (
                deepdiff.DeepDiff(
                    function_spec["inputs"], expected_inputs, ignore_order=True
                )
                == {}
            )
        if expected_hyper_params:
            assert (
                deepdiff.DeepDiff(
                    function_spec["hyperparams"],
                    expected_hyper_params,
                    ignore_order=True,
                )
                == {}
            )
        if expected_secrets:
            assert (
                deepdiff.DeepDiff(
                    function_spec["secret_sources"],
                    [expected_secrets],
                    ignore_order=True,
                )
                == {}
            )

    @staticmethod
    def _assert_pod_env(pod_env, expected_variables):
        for env_variable in pod_env:
            if isinstance(env_variable, V1EnvVar):
                env_variable = dict(name=env_variable.name, value=env_variable.value)
            name = env_variable["name"]
            if name in expected_variables:
                if expected_variables[name]:
                    assert expected_variables[name] == env_variable["value"]
                expected_variables.pop(name)

        # Make sure all variables were accounted for
        assert len(expected_variables) == 0

    def _assert_v3io_mount_configured(self, v3io_user, v3io_access_key):
        args, _ = get_k8s().v1api.create_namespaced_pod.call_args
        pod_spec = args[1].spec
        container_spec = pod_spec.containers[0]

        pod_env = container_spec.env
        self._assert_pod_env(
            pod_env,
            {
                "V3IO_API": None,
                "V3IO_USERNAME": v3io_user,
                "V3IO_ACCESS_KEY": v3io_access_key,
            },
        )

        expected_volume = {
            "flexVolume": {
                "driver": "v3io/fuse",
                "options": {"accessKey": v3io_access_key},
            },
            "name": "v3io",
        }
        assert (
            deepdiff.DeepDiff(pod_spec.volumes[0], expected_volume, ignore_order=True)
            == {}
        )

        expected_volume_mounts = [
            {"mountPath": "/v3io", "name": "v3io", "subPath": ""},
            {"mountPath": "/User", "name": "v3io", "subPath": f"users/{v3io_user}"},
        ]
        assert (
            deepdiff.DeepDiff(
                container_spec.volume_mounts, expected_volume_mounts, ignore_order=True
            )
            == {}
        )

    def _assert_pvc_mount_configured(self, pvc_name, pvc_mount_path, volume_name):
        args, _ = get_k8s().v1api.create_namespaced_pod.call_args
        pod_spec = args[1].spec

        expected_volume = {
            "name": volume_name,
            "persistentVolumeClaim": {"claimName": pvc_name},
        }
        assert (
            deepdiff.DeepDiff(pod_spec.volumes[0], expected_volume, ignore_order=True)
            == {}
        )

        expected_volume_mounts = [
            {"mountPath": pvc_mount_path, "name": volume_name},
        ]

        container_spec = pod_spec.containers[0]
        assert (
            deepdiff.DeepDiff(
                container_spec.volume_mounts, expected_volume_mounts, ignore_order=True
            )
            == {}
        )

    def _assert_secret_mount(self, volume_name, secret_name, default_mode, mount_path):
        args, _ = get_k8s().v1api.create_namespaced_pod.call_args
        pod_spec = args[1].spec

        expected_volume = {
            "name": volume_name,
            "secret": {"defaultMode": default_mode, "secretName": secret_name},
        }
        assert (
            deepdiff.DeepDiff(pod_spec.volumes[0], expected_volume, ignore_order=True)
            == {}
        )

        expected_volume_mounts = [
            {"mountPath": mount_path, "name": volume_name},
        ]

        container_spec = pod_spec.containers[0]
        assert (
            deepdiff.DeepDiff(
                container_spec.volume_mounts, expected_volume_mounts, ignore_order=True
            )
            == {}
        )

    def _assert_pod_create_called(
        self,
        expected_runtime_class_name="job",
        expected_params=None,
        expected_inputs=None,
        expected_hyper_params=None,
        expected_secrets=None,
        expected_limits=None,
        expected_requests=None,
        expected_code=None,
        expected_env={},
    ):
        create_pod_mock = get_k8s().v1api.create_namespaced_pod
        create_pod_mock.assert_called_once()
        args, _ = create_pod_mock.call_args
        assert args[0] == self.namespace
        pod_spec = args[1]
        self._assert_labels(pod_spec.metadata.labels, expected_runtime_class_name)

        container_spec = pod_spec.spec.containers[0]

        if expected_limits:
            assert (
                deepdiff.DeepDiff(
                    container_spec.resources["limits"],
                    expected_limits,
                    ignore_order=True,
                )
                == {}
            )
        if expected_requests:
            assert (
                deepdiff.DeepDiff(
                    container_spec.resources["requests"],
                    expected_requests,
                    ignore_order=True,
                )
                == {}
            )

        pod_env = container_spec.env

        expected_code_found = False

        expected_env["MLRUN_NAMESPACE"] = self.namespace
        self._assert_pod_env(pod_env, expected_env)
        for env_variable in pod_env:
            if isinstance(env_variable, V1EnvVar):
                env_variable = dict(name=env_variable.name, value=env_variable.value)
            if env_variable["name"] == "MLRUN_EXEC_CONFIG":
                function_config = json.loads(env_variable["value"])
                self._assert_function_config(
                    function_config,
                    expected_params,
                    expected_inputs,
                    expected_hyper_params,
                    expected_secrets,
                )

            if expected_code and env_variable["name"] == "MLRUN_EXEC_CODE":
                assert env_variable["value"] == b64encode(
                    expected_code.encode("utf-8")
                ).decode("utf-8")
                expected_code_found = True

        if expected_code:
            assert expected_code_found

        assert pod_spec.spec.containers[0].image == self.image_name
