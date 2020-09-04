import logging

from crontab import CronTab


class AppJob:
    def __init__(self, command: str, app: str, comment: str, project: str = None):
        self.command = command
        self.app = app
        self.project = project
        self.comment = comment
        self.cron_job = None

    @classmethod
    def comment_prefix(cls, app: str, project: str = None):
        if project:
            return "{}({})".format(app, project)
        return "{}".format(app)

    @property
    def cron_comment(self):
        return "{}: {}".format(self.comment_prefix(self.app, self.project), self.comment)

    def is_valid(self):
        return self.cron_job and self.cron_job.is_valid()


class AppCronTab:
    def __init__(self, app: str, user: str, log: logging.Logger = None):
        self.app = app
        self.cron = CronTab(user=user if user else True)
        self.log = log

    def jobs(self, project: str = None):
        return [job for job in self.cron if job.comment.startswith(AppJob.comment_prefix(self.app, project))]

    def job(self, command: str, comment: str, frequency: str = None, project: str = None):
        app_job = AppJob(command, self.app, comment, project)
        job = next(self.cron.find_comment(app_job.cron_comment), None)
        if job:
            if self.log:
                self.log.info("found cron job for {} and clearing".format(app_job.comment))
            job.clear()
            job.command = command
        else:
            if self.log:
                self.log.info("creating new cron job for " + app_job.comment)
            job = self.cron.new(command=app_job.command, comment=app_job.cron_comment)
        if frequency:
            job.setall(frequency)
            if self.log:
                self.log.info("set job with frequency: {}".format(frequency))
        if self.log:
            self.log.debug(job)
        app_job.cron_job = job
        return app_job

    def write(self):
        self.cron.write()
