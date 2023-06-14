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
import typing

import sqlalchemy.orm

import mlrun.api.api.utils
import mlrun.api.utils.singletons.db
import mlrun.utils.singleton
from mlrun.utils import logger


class Alerts(
    metaclass=mlrun.utils.singleton.Singleton,
):
    def create_alert(
        self,
        session: sqlalchemy.orm.Session,
        name: str,
        project: str,
        alert_data: mlrun.common.schemas.AlertConfig,
    ):
        project = project or mlrun.mlconf.default_project

        if (
            mlrun.api.utils.singletons.db.get_db().get_alert(session, project, name)
            is not None
        ):
            raise mlrun.errors.MLRunConflictError(
                f"Alert {name} for project {project} already exists"
            )

        self._validate_alert(alert_data, None, project)

        alert_data.created = datetime.datetime.now(datetime.timezone.utc)
        new_alert = mlrun.api.utils.singletons.db.get_db().create_alert(
            session, alert_data
        )
        for kind in new_alert.trigger.events:
            mlrun.api.crud.Events().add_event(project, kind, new_alert.id)

        mlrun.api.api.utils.validate_and_mask_notification_list(
            alert_data.notifications, alert_data.entity.id, project
        )

        mlrun.api.utils.singletons.db.get_db().enrich_alert(session, new_alert)

        return new_alert

    def store_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        alert_id: int,
        alert_data: mlrun.common.schemas.AlertConfig,
    ):
        project = project or mlrun.mlconf.default_project

        alert = mlrun.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )
        if alert is None:
            raise mlrun.errors.MLRunNotFoundError(
                f"Alert {alert_id} for project {project} does not exist"
            )

        if alert.name != alert_data.name:
            raise mlrun.errors.MLRunBadRequestError(
                f"Alert name change not allowed for alert {alert_id} for project {project}"
            )

        self._validate_alert(alert, alert_id, project)

        alert_data.created = alert.created
        alert_data.id = alert.id
        new_alert = mlrun.api.utils.singletons.db.get_db().store_alert(
            session, alert_data
        )

        for kind in alert.trigger.events:
            mlrun.api.crud.Events().remove_event(project, kind)

        for kind in new_alert.trigger.events:
            mlrun.api.crud.Events().add_event(project, kind, alert.id)

        self._delete_notifications(alert)

        mlrun.api.api.utils.validate_and_mask_notification_list(
            alert_data.notifications, alert_data.entity.id, project
        )

        self.reset_alert(session, project, alert.id)

        mlrun.api.utils.singletons.db.get_db().enrich_alert(session, new_alert)

        return new_alert

    def list_alerts(
        self,
        session: sqlalchemy.orm.Session,
        project: str = "",
    ) -> typing.List[mlrun.common.schemas.AlertConfig]:
        project = project or mlrun.mlconf.default_project
        return mlrun.api.utils.singletons.db.get_db().list_alerts(session, project)

    def get_enriched_alert_by_id(self, session: sqlalchemy.orm.Session, alert_id: str):
        alert = mlrun.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )
        if alert is not None:
            mlrun.api.utils.singletons.db.get_db().enrich_alert(session, alert)
        return alert

    def get_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        name: str,
    ) -> mlrun.common.schemas.AlertConfig:
        project = project or mlrun.mlconf.default_project
        return mlrun.api.utils.singletons.db.get_db().get_alert(session, project, name)

    def get_alert_by_id(
        self,
        session: sqlalchemy.orm.Session,
        alert_id: str,
    ) -> mlrun.common.schemas.AlertConfig:
        return mlrun.api.utils.singletons.db.get_db().get_alert_by_id(session, alert_id)

    def delete_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        alert_id: int,
    ):
        project = project or mlrun.mlconf.default_project

        alert = mlrun.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )

        if alert is None:
            return

        for kind in alert.trigger.events:
            mlrun.api.crud.Events().remove_event(project, kind)

        mlrun.api.utils.singletons.db.get_db().delete_alert(session, alert_id)

    def process_event(
        self,
        session: sqlalchemy.orm.Session,
        alert_id: int,
        event_data: mlrun.common.schemas.Event,
    ):
        state = mlrun.api.utils.singletons.db.get_db().get_alert_state(
            session, alert_id
        )
        if state.active:
            return

        alert = mlrun.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )

        state.count += 1
        state_obj = None
        if alert.entity.id == "*" or alert.entity.id == event_data.entity.id:
            send_notification = False
            if alert.criteria is not None:
                if alert.criteria.period is not None:
                    state_obj = state.full_object

                    if state_obj is None:
                        state_obj = {"events": [event_data.timestamp]}
                    else:
                        state_obj["events"].append(event_data.timestamp)
                        self._normalize_events(
                            state_obj, self._string2datetime(alert.criteria.period)
                        )

                    state.count = len(state_obj["events"])
                    if state.count >= alert.criteria.count:
                        send_notification = True
                else:
                    if state.count >= alert.criteria.count:
                        send_notification = True
            else:
                send_notification = True

            if send_notification:
                logger.debug(
                    f"Sending notification {alert.name} !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                )

                if alert.reset_policy == "auto":
                    self.reset_alert(session, alert_id)
                else:
                    mlrun.api.utils.singletons.db.get_db().store_alert_state(
                        session,
                        alert_id,
                        count=state.count,
                        last_updated=event_data.timestamp,
                        active=True,
                        obj=state_obj,
                    )
            else:
                mlrun.api.utils.singletons.db.get_db().store_alert_state(
                    session,
                    alert_id,
                    count=state.count,
                    last_updated=event_data.timestamp,
                    obj=state_obj,
                )

    @staticmethod
    def _validate_alert(alert, alert_id, project):
        if alert.criteria is not None and alert.criteria.period is not None:
            if Alerts._string2datetime(alert.criteria.period) is None:
                raise mlrun.errors.MLRunBadRequestError(
                    f"Invalid period ({alert.criteria.period}) specified for for alert {alert_id} for project {project}"
                )

        for notification in alert.notifications:
            if notification.kind not in [
                mlrun.common.schemas.NotificationKind.git,
                mlrun.common.schemas.NotificationKind.slack,
                mlrun.common.schemas.NotificationKind.webhook,
            ]:
                raise mlrun.errors.MLRunBadRequestError(
                    f"Unsupported notification ({notification.kind}) for alert {alert_id} for project {project}"
                )

    @staticmethod
    def _string2datetime(date_str):
        date_str = date_str.strip().lower()
        s = 0
        m = 0
        h = 0
        d = 0
        if date_str.endswith("d"):
            d = int(date_str.split("d")[0].strip())
        elif date_str.endswith("h"):
            h = int(date_str.split("h")[0].strip())
        elif date_str.endswith("m"):
            m = int(date_str.split("m")[0].strip())
        elif date_str.endswith("s"):
            s = int(date_str.split("s")[0].strip())

        if s == 0 and m == 0 and h == 0 and d == 0:
            return None

        return datetime.timedelta(days=d, hours=h, minutes=m, seconds=s)

    @staticmethod
    def _normalize_events(obj, period):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        events = obj["events"]
        for event in events:
            if isinstance(event, str):
                event_time = datetime.datetime.fromisoformat(event)
            else:
                event_time = event
            if event_time > now + period:
                events.remove(event)

    def reset_alert(self, session: sqlalchemy.orm.Session, project: str, alert_id: int):

        alert = mlrun.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )
        if alert is None:
            raise mlrun.errors.MLRunNotFoundError(
                f"Alert {alert_id} for project {project} does not exist"
            )

        mlrun.api.utils.singletons.db.get_db().store_alert_state(
            session, alert_id, count=0, last_updated=None
        )

    def _delete_notifications(self, alert: mlrun.common.schemas.AlertConfig):
        for notification in alert.notifications:
            mlrun.api.api.utils.delete_notification_params_secret(
                alert.project, notification
            )
