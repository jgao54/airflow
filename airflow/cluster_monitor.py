# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime, timedelta
import os
from time import sleep

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, or_

from airflow import models, jobs, settings
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.utils.logging import LoggingMixin

Base = models.Base
ID_LEN = models.ID_LEN
Stats = settings.Stats


class ClusterMonitor(Base, LoggingMixin):
    """
    ClusterStatus stores the healthiness of scheduler instances for monitoring,
    where each cluster is represented uniquely by the combination of
    service_type, hostname, and listening_port
    """
    __tablename__ = "cluster_monitor"

    id = Column(Integer(), primary_key=True)
    job_id = Column(Integer, ForeignKey('job.id'))
    pid = Column(Integer())
    job_type = Column(String(50), ForeignKey('job.job_type'))
    state = Column(String(50), ForeignKey('job.state'))
    created_at = Column(DateTime())
    num_runs = Column(Integer(), ForeignKey('job.num_runs'))
    num_runs_left = Column(Integer())
    latest_heartbeat = Column(DateTime)
    seconds_since_latest_heartbeat = Column(Integer())

    def __init__(
            self,
            name=None,
            pid=None,
            job_id=None,
            job_type=None,
            state=None,
            created_at=None,
            num_runs=None,
            latest_heartbeat=None,
            seconds_since_latest_heartbeat=None):
        self.name = name,
        self.pid = pid
        self.job_id = job_id,
        self.job_type = job_type
        self.state = state
        self.created_at = created_at
        self.num_runs = num_runs
        self.latest_heartbeat = latest_heartbeat
        self.seconds_since_latest_heartbeat = seconds_since_latest_heartbeat

    def __repr__(self):
        return str((
            'pid: {pid}\n job id: {job_id} \n job type: {job_type}\n state: {state}\n '
            'created_at: {created_at}\n latest_heartbeat: {latest_heartbeat}'
        ).format(
            pid=self.id,
            job_id=self.job_id,
            job_type=self.job_type,
            state=self.state,
            created_at=self.created_at,
            latest_heartbeat=self.latest_heartbeat
        ))

    # TODO: can/should this method be static?
    @provide_session
    def create(self, pid, num_runs, name='Default', session=None):
        """
        Called by the scheduler to add a row in the cluster monito
        """

        # make sure the cluster with the particular pid  doesn't already exists
        CM = ClusterMonitor
        cluster = session.query(CM).filter(CM.pid == pid).first()
        if cluster:
            # TODO: might not want to throw error here, but clean up old pid
            raise AirflowException("PID {} already in use.".format(pid))

        cluster = ClusterMonitor(pid=pid, name=name, num_runs=num_runs)
        cluster.job_type = 'SchedulerJob'
        cluster.state = State.RUNNING
        cluster.created_at = datetime.now()
        cluster.latest_heartbeat = datetime.now()
        cluster.seconds_since_latest_heartbeat = 0

        session.commit(cluster)

    @provide_session
    def run(self, session=None):
        """
        Run continuously to check if all the pids are alive or not, started by webserver
        """
        CM = ClusterMonitor
        SJ = jobs.SchedulerJob

        # TODO: sleeping interval should be configurable in airflow.cfg
        interval = 0

        while True:
            clusters = session.query(CM).all()
            for cluster in clusters:

                # get the corresponding row in schedulerJob table and make sure it exists
                sj = session.query(SJ).filter(
                    SJ.job_id == cluster.job_id
                ).first()

                if not sj:
                    raise AirflowException("No job available!")

                # if pid no longer exists in OS....
                if not self.check_pid(cluster.pid):

                    # update Job table state to 'SHUTDOWN'
                    sj.state = State.SHUTDOWN
                    session.merge(sj)
                    session.commit()

                    # update cluster state to 'SHUTDOWN' as well
                    cluster.state = State.SHUTDOWN
                    session.merge(cluster)
                    session.commit()

                    continue

                # scheduler process is alive, but been stuck in a loop for a long
                # time, change state to stuck (or something equivalent)
                secs = (configuration.getint('scheduler', 'job_heartbeat_sec') * 3) + 120
                limit_dttm = datetime.now() - timedelta(seconds=secs)
                if sj.state == State.RUNNING and sj.latest_heartbeat < limit_dttm:
                    cluster.state = State.STUCK

                # scheduler process seems healthy, update the latest heartbeat to
                # match job table
                else:
                    cluster.latest_heartbeat = sj.latest_heartbeat

                # update the num of seconds since last heartbeat
                cluster.seconds_since_latest_heartbeat = (
                    (datetime.now() - cluster.latest_heartbeat).total_seconds()
                )
                session.merge(cluster)
                session.commit()

            sleep(interval)

    # TODO: os.kill() is Unix only, not supported for Windows, alternative?
    # TODO: also check if this os-level approach is safe in general
    @staticmethod
    def check_pid(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    @staticmethod
    def shut_down(self, pid):
        """
        Can potentially support shutting down a scheduler later on
        """
        pass