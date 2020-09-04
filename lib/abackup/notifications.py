from enum import Enum
from typing import Any, Dict, List

import requests


class Mode(Enum):
    AUTO = "auto"
    ALWAYS = "always"
    NEVER = "never"


class SlackResponse:
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message

    def is_error(self):
        return self.code != 200


class SlackField:
    def __init__(self, title: str, value: str, short: bool = True):
        self.title = title
        self.value = value
        self.short = short

    def to_data(self):
        return {"title": self.title, "value": self.value, "short": self.short}


class SlackAttachment:
    def __init__(self, fallback=None, color=None, pretext=None, author_name=None, author_link=None, author_icon=None,
                 title=None, title_link=None, text=None, fields=None, image_url=None, thumb_url=None, footer=None,
                 footer_icon=None, ts=None):
        self.fallback = fallback
        self.color = color
        self.pretext = pretext
        self.author_name = author_name
        self.author_link = author_link
        self.author_icon = author_icon
        self.title = title
        self.title_link = title_link
        self.text = text
        self.fields = fields
        self.image_url = image_url
        self.thumb_url = thumb_url
        self.footer = footer
        self.footer_icon = footer_icon
        self.ts = ts

    def to_data(self):
        data = {"mrkdwn_in": ["text", "pretext", "fields"]}
        for k, v in self.__dict__.items():
            if v:
                if k == "fields":
                    data[k] = [field.to_data() for field in v]
                else:
                    data[k] = v
        return data


class SlackData:
    def __init__(self, text: str = None, attachments: List[SlackAttachment] = None, username: str = None,
                 channel: str = None):
        self.text = text
        self.attachments = attachments
        self.username = username
        self.channel = channel

    def to_data(self):
        data = {}
        for k, v in self.__dict__.items():
            if v:
                if k == "attachments":
                    data[k] = [attachment.to_data() for attachment in v]
                else:
                    data[k] = v
        return data


class Severity(Enum):
    ERROR = "error"
    CRITICAL = "critical"
    GOOD = "good"


def build_slack_data(title: str, severity: Severity, description: str = None, fields: Dict[str, Any] = None,
                     file_name: str = None, line_number: int = None, time: float = None):
    if fields is None:
        fields = {}
    attachment = SlackAttachment(title=title, color="good" if severity == Severity.GOOD else "danger",
                                 text=description, footer="{}:{}".format(file_name, line_number), ts=time,
                                 fields=[SlackField(k, v) for k, v in fields.items()], fallback=title)
    return SlackData(attachments=[attachment])


class SlackNotifier:
    def __init__(self, api_url: str, username: str = None, channel: str = None):
        self.api_url = api_url
        self.username = username
        self.channel = channel

    def notify_raw(self, data: SlackData):
        data.username = self.username
        data.channel = self.channel
        response = requests.post(self.api_url, json=data.to_data())
        return SlackResponse(response.status_code, response.text)

    def notify(self, title: str, severity: Severity, description: str = None, fields: Dict[str, Any] = None,
               file_name: str = None, line_number: int = None, time: float = None):
        if not fields:
            fields = {}
        return self.notify_raw(build_slack_data(title, severity, description, fields, file_name, line_number, time))
