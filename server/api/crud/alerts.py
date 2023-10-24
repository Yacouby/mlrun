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

import mlrun.utils.singleton
import server.api.api.utils
import server.api.utils.singletons.db
from mlrun.utils import logger
from server.api.utils.notification_pusher import AlertNotificationPusher


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
            server.api.utils.singletons.db.get_db().get_alert(session, project, name)
            is not None
        ):
            raise mlrun.errors.MLRunConflictError(
                f"Alert {name} for project {project} already exists"
            )

        self._validate_alert(alert_data, None, project)

        alert_data.notifications = [
            mlrun.common.schemas.notification.Notification(**x.to_dict())
            for x in server.api.api.utils.validate_and_mask_notification_list(
                alert_data.notifications, None, project
            )
        ]

        alert_data.created = datetime.datetime.now(datetime.timezone.utc)
        new_alert = server.api.utils.singletons.db.get_db().create_alert(
            session, alert_data
        )
        for kind in new_alert.trigger.events:
            server.api.crud.Events().add_event(project, kind, new_alert.id)

        server.api.utils.singletons.db.get_db().enrich_alert(session, new_alert)

        return new_alert

    def store_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        alert_id: int,
        alert_data: mlrun.common.schemas.AlertConfig,
    ):
        project = project or mlrun.mlconf.default_project

        alert = server.api.utils.singletons.db.get_db().get_alert_by_id(
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

        for kind in alert.trigger.events:
            server.api.crud.Events().remove_event(project, kind)

        self._delete_notifications(alert)

        alert_data.created = alert.created
        alert_data.id = alert.id

        alert_data.notifications = [
            mlrun.common.schemas.notification.Notification(**x.to_dict())
            for x in server.api.api.utils.validate_and_mask_notification_list(
                alert_data.notifications, None, project
            )
        ]

        new_alert = server.api.utils.singletons.db.get_db().store_alert(
            session, alert_data
        )

        for kind in new_alert.trigger.events:
            server.api.crud.Events().add_event(project, kind, alert.id)

        self.reset_alert(session, project, alert.id)

        server.api.utils.singletons.db.get_db().enrich_alert(session, new_alert)

        return new_alert

    def list_alerts(
        self,
        session: sqlalchemy.orm.Session,
        project: str = "",
    ) -> typing.List[mlrun.common.schemas.AlertConfig]:
        project = project or mlrun.mlconf.default_project
        return server.api.utils.singletons.db.get_db().list_alerts(session, project)

    def get_enriched_alert_by_id(self, session: sqlalchemy.orm.Session, alert_id: str):
        alert = server.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )
        if alert is not None:
            server.api.utils.singletons.db.get_db().enrich_alert(session, alert)
        return alert

    def get_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        name: str,
    ) -> mlrun.common.schemas.AlertConfig:
        project = project or mlrun.mlconf.default_project
        return server.api.utils.singletons.db.get_db().get_alert(session, project, name)

    def get_alert_by_id(
        self,
        session: sqlalchemy.orm.Session,
        alert_id: str,
    ) -> mlrun.common.schemas.AlertConfig:
        return server.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )

    def delete_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        alert_id: int,
    ):
        project = project or mlrun.mlconf.default_project

        alert = server.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )

        if alert is None:
            return

        for kind in alert.trigger.events:
            server.api.crud.Events().remove_event(project, kind)

        server.api.utils.singletons.db.get_db().delete_alert(session, alert_id)

    def process_event(
        self,
        session: sqlalchemy.orm.Session,
        alert_id: int,
        event_data: mlrun.common.schemas.Event,
    ):
        state = server.api.utils.singletons.db.get_db().get_alert_state(
            session, alert_id
        )
        if state.active:
            return

        alert = server.api.utils.singletons.db.get_db().get_alert_by_id(
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
                AlertNotificationPusher().push(alert, event_data)

                if alert.reset_policy == "auto":
                    self.reset_alert(session, alert.project, alert_id)
                else:
                    server.api.utils.singletons.db.get_db().store_alert_state(
                        session,
                        alert_id,
                        count=state.count,
                        last_updated=event_data.timestamp,
                        active=True,
                        obj=state_obj,
                    )
            else:
                server.api.utils.singletons.db.get_db().store_alert_state(
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
            notification_object = mlrun.model.Notification.from_dict(
                notification.dict()
            )
            notification_object.validate_notification()

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

        alert = server.api.utils.singletons.db.get_db().get_alert_by_id(
            session, alert_id
        )
        if alert is None:
            raise mlrun.errors.MLRunNotFoundError(
                f"Alert {alert_id} for project {project} does not exist"
            )

        server.api.utils.singletons.db.get_db().store_alert_state(
            session, alert_id, count=0, last_updated=None
        )

    def _delete_notifications(self, alert: mlrun.common.schemas.AlertConfig):
        for notification in alert.notifications:
            server.api.api.utils.delete_notification_params_secret(
                alert.project, notification
            )
