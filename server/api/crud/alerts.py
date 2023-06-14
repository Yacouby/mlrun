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
            server.api.crud.Events().add_event(project, kind, new_alert.name)

        server.api.utils.singletons.db.get_db().enrich_alert(session, new_alert)

        return new_alert

    def store_alert(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        name: str,
        alert_data: mlrun.common.schemas.AlertConfig,
    ):
        project = project or mlrun.mlconf.default_project

        alert = server.api.utils.singletons.db.get_db().get_alert(
            session, project, name
        )
        if alert is None:
            raise mlrun.errors.MLRunNotFoundError(
                f"Alert {name} for project {project} does not exist"
            )

        if alert.name != alert_data.name:
            raise mlrun.errors.MLRunBadRequestError(
                f"Alert name change not allowed for alert {name} for project {project}"
            )

        self._validate_alert(alert, name, project)

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
            server.api.crud.Events().add_event(project, kind, alert.name)

        self.reset_alert(session, project, alert.name)

        server.api.utils.singletons.db.get_db().enrich_alert(session, new_alert)

        return new_alert

    def list_alerts(
        self,
        session: sqlalchemy.orm.Session,
        project: str = "",
    ) -> list[mlrun.common.schemas.AlertConfig]:
        project = project or mlrun.mlconf.default_project
        return server.api.utils.singletons.db.get_db().list_alerts(session, project)

    def get_enriched_alert(
        self, session: sqlalchemy.orm.Session, project: str, name: str
    ):
        alert = server.api.utils.singletons.db.get_db().get_alert(
            session, project, name
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
        name: str,
    ):
        project = project or mlrun.mlconf.default_project

        alert = server.api.utils.singletons.db.get_db().get_alert(
            session, project, name
        )

        if alert is None:
            return

        for kind in alert.trigger.events:
            server.api.crud.Events().remove_event(project, kind)

        server.api.utils.singletons.db.get_db().delete_alert(session, project, name)

    def process_event(
        self,
        session: sqlalchemy.orm.Session,
        project: str,
        name: str,
        event_data: mlrun.common.schemas.Event,
    ):
        alert = server.api.utils.singletons.db.get_db().get_alert(
            session, project, name
        )

        state = server.api.utils.singletons.db.get_db().get_alert_state(
            session, alert.id
        )
        if state.active:
            return

        state_obj = None
        if alert.entity.id in ["*", event_data.entity.id]:
            send_notification = False
            if alert.criteria is not None:
                state_obj = state.full_object

                if state_obj is None:
                    state_obj = {"events": [event_data.timestamp]}

                if alert.criteria.period is not None:
                    state_obj["events"].append(event_data.timestamp)
                    self._normalize_events(
                        state_obj, self._string2datetime(alert.criteria.period)
                    )

                if len(state_obj["events"]) >= alert.criteria.count:
                    send_notification = True
            else:
                send_notification = True

            active = False
            if send_notification:
                state.count += 1
                logger.debug(f"Sending notifications for alert {alert.name}")
                AlertNotificationPusher().push(alert, event_data)

                if alert.reset_policy == "auto":
                    self.reset_alert(session, alert.project, alert.name)
                else:
                    active = True

            server.api.utils.singletons.db.get_db().store_alert_state(
                session,
                alert.project,
                alert.name,
                count=state.count,
                last_updated=event_data.timestamp,
                obj=state_obj,
                active=active,
            )

    @staticmethod
    def _validate_alert(alert, name, project):
        if (
            alert.criteria is not None
            and alert.criteria.period is not None
            and Alerts._string2datetime(alert.criteria.period) is None
        ):
            raise mlrun.errors.MLRunBadRequestError(
                f"Invalid period ({alert.criteria.period}) specified for for alert {name} for project {project}"
            )

        for notification in alert.notifications:
            if notification.kind not in [
                mlrun.common.schemas.NotificationKind.git,
                mlrun.common.schemas.NotificationKind.slack,
                mlrun.common.schemas.NotificationKind.webhook,
            ]:
                raise mlrun.errors.MLRunBadRequestError(
                    f"Unsupported notification ({notification.kind}) for alert {name} for project {project}"
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

    def reset_alert(self, session: sqlalchemy.orm.Session, project: str, name: str):
        alert = server.api.utils.singletons.db.get_db().get_alert(
            session, project, name
        )
        if alert is None:
            raise mlrun.errors.MLRunNotFoundError(
                f"Alert {name} for project {project} does not exist"
            )

        server.api.utils.singletons.db.get_db().store_alert_state(
            session, project, name, last_updated=None
        )

    def _delete_notifications(self, alert: mlrun.common.schemas.AlertConfig):
        for notification in alert.notifications:
            server.api.api.utils.delete_notification_params_secret(
                alert.project, notification
            )
