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
import datetime

import sqlalchemy.orm

import mlrun.api.api.utils
import mlrun.api.utils.singletons.db
import mlrun.utils.singleton
from mlrun.utils import logger


class Events(
    metaclass=mlrun.utils.singleton.Singleton,
):

    _cache = dict()

    @staticmethod
    def is_valid_event(project: str, event_data: mlrun.common.schemas.Event):
        if event_data.entity.project != project:
            return False

        if not event_data.is_valid():
            return False

        return True

    def add_event(self, project, name, alert_id):
        self._cache.setdefault((project, name), []).append(alert_id)

    def remove_event(self, project, name):
        del self._cache[(project, name)]

    def process_event(
        self,
        session: sqlalchemy.orm.Session,
        event_data: mlrun.common.schemas.Event,
        event_name: str,
        project: str = None,
    ):
        project = project or mlrun.mlconf.default_project

        event_data.timestamp = datetime.datetime.now(datetime.timezone.utc)

        try:
            for alert_id in self._cache[(project, event_name)]:
                mlrun.api.crud.Alerts().process_event(session, alert_id, event_data)
        except KeyError:
            logger.warn(f"Received unknown event {event_name}", project=project)
            return
