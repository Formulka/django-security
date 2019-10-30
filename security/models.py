import re
import json
from json import JSONDecodeError

from datetime import timedelta

from ipware.ip import get_ip
from jsonfield import JSONField

from django.conf import settings as django_settings
from django.contrib.contenttypes.models import ContentType
from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.template.defaultfilters import truncatechars
from django.urls import resolve, reverse
from django.urls.exceptions import Resolver404
from django.utils import timezone
from django.utils.encoding import force_text
from django.utils.translation import ugettext_lazy as _
from django.utils.timezone import localtime

try:
    from django.core.urlresolvers import get_resolver
except ImportError:
    from django.urls import get_resolver

from chamber.models import SmartModel

from enumfields import NumEnumField

from security.config import settings
from security.utils import get_headers, remove_nul_from_string, regex_sub_groups_global, is_base_collection

try:
    from django.contrib.contenttypes.fields import GenericForeignKey
except ImportError:
    from django.contrib.contenttypes.generic import GenericForeignKey

try:
    from pyston.filters.default_filters import CaseSensitiveStringFieldFilter
except ImportError:
    CaseSensitiveStringFieldFilter = object

from .enums import InputLoggedRequestType, LoggedRequestStatus, CeleryTaskLogState


def display_json(value, indent=4):
    return json.dumps(value, indent=indent, ensure_ascii=False, cls=DjangoJSONEncoder)


def hide_sensitive_data_body(content):
    for pattern in settings.HIDE_SENSITIVE_DATA_PATTERNS.get('BODY', ()):
        content = regex_sub_groups_global(pattern, settings.SENSITIVE_DATA_REPLACEMENT, content)
    return content


def hide_sensitive_data_headers(headers):
    headers = dict(headers)
    for pattern in settings.HIDE_SENSITIVE_DATA_PATTERNS.get('HEADERS', ()):
        for header_name, header in headers.items():
            if re.match(pattern, header_name, re.IGNORECASE):
                headers[header_name] = settings.SENSITIVE_DATA_REPLACEMENT
    return headers


def hide_sensitive_data_queries(queries):
    queries = dict(queries)
    for pattern in settings.HIDE_SENSITIVE_DATA_PATTERNS.get('QUERIES', ()):
        for query_name, query in queries.items():
            if re.match(pattern, query_name, re.IGNORECASE):
                queries[query_name] = (
                    len(query) * [settings.SENSITIVE_DATA_REPLACEMENT] if is_base_collection(query)
                    else settings.SENSITIVE_DATA_REPLACEMENT
                )
    return queries


def truncate_json_data(data):
    if isinstance(data, dict):
        return {key: truncate_json_data(val) for key, val in data.items()}
    elif isinstance(data, list):
        return [truncate_json_data(val) for val in data]
    elif isinstance(data, str):
        return truncatechars(data, settings.LOG_JSON_STRING_LENGTH)
    else:
        return data


def truncate_body(content, max_lenght):
    content = force_text(content, errors='replace')
    if len(content) > max_lenght:
        try:
            json_content = json.loads(content)
            return (
                json.dumps(truncate_json_data(json_content))
                if isinstance(json_content, (dict, list)) and settings.LOG_JSON_STRING_LENGTH is not None
                else content[:max_lenght + 1]
            )
        except JSONDecodeError:
            return content[:max_lenght + 1]
    else:
        return content


def clean_body(body, max_lenght):
    cleaned_body = truncatechars(
        truncate_body(body, max_lenght), max_lenght + len(settings.SENSITIVE_DATA_REPLACEMENT)
    ) if max_lenght is not None else str(body)
    cleaned_body = hide_sensitive_data_body(remove_nul_from_string(cleaned_body)) if cleaned_body else cleaned_body
    cleaned_body = truncatechars(cleaned_body, max_lenght) if max_lenght else cleaned_body
    return cleaned_body


def clean_headers(headers):
    return hide_sensitive_data_headers(headers) if headers else headers


def clean_queries(queries):
    return hide_sensitive_data_queries(queries) if queries else queries


class InputLoggedRequestManager(models.Manager):
    """
    Create new LoggedRequest instance from HTTP request
    """

    def prepare_from_request(self, request):
        user = hasattr(request, 'user') and request.user.is_authenticated and request.user or None
        path = truncatechars(request.path, 200)

        try:
            slug = resolve(request.path_info, getattr(request, 'urlconf', None)).view_name
        except Resolver404:
            slug = None

        return self.model(
            request_headers=clean_headers(get_headers(request)),
            request_body=clean_body(request.body, settings.LOG_REQUEST_BODY_LENGTH),
            user=user,
            method=request.method.upper()[:7],
            host=request.get_host(),
            path=path,
            queries=clean_queries(request.GET.dict()),
            is_secure=request.is_secure(),
            ip=get_ip(request),
            request_timestamp=timezone.now(),
            slug=slug
        )


class LoggedRequest(SmartModel):

    host = models.CharField(_('host'), max_length=255, null=False, blank=False, db_index=True)
    host._filter = CaseSensitiveStringFieldFilter
    method = models.SlugField(_('method'), max_length=7, null=False, blank=False, db_index=True)
    path = models.CharField(_('URL path'), max_length=255, null=False, blank=True, db_index=True)
    path._filter = CaseSensitiveStringFieldFilter
    queries = JSONField(_('queries'), null=True, blank=True)
    is_secure = models.BooleanField(_('HTTPS connection'), default=False, null=False, blank=False)
    slug = models.CharField(_('slug'), null=True, blank=True, db_index=True, max_length=255)

    # Request information
    request_timestamp = models.DateTimeField(_('request timestamp'), null=False, blank=False, db_index=True)
    request_headers = JSONField(_('request headers'), null=True, blank=True)
    request_body = models.TextField(_('request body'), null=False, blank=True)

    # Response information
    response_timestamp = models.DateTimeField(_('response timestamp'), null=True, blank=True)
    response_code = models.PositiveSmallIntegerField(_('response code'), null=True, blank=True)
    response_headers = JSONField(_('response headers'), null=True, blank=True)
    response_body = models.TextField(_('response body'), null=True, blank=True)

    status = NumEnumField(verbose_name=_('status'), enum=LoggedRequestStatus, null=False, blank=False,
                          default=LoggedRequestStatus.INCOMPLETE)
    error_description = models.TextField(_('error description'), null=True, blank=True)
    exception_name = models.CharField(_('exception name'), null=True, blank=True, max_length=255)

    @classmethod
    def get_status(cls, status_code):
        if status_code >= 500:
            return LoggedRequestStatus.ERROR
        elif status_code >= 400:
            return LoggedRequestStatus.WARNING
        else:
            return LoggedRequestStatus.INFO

    def short_queries(self):
        return truncatechars(display_json(self.queries, indent=0), 50)
    short_queries.short_description = _('queries')
    short_queries.filter_by = 'queries'

    def short_request_headers(self):
        return truncatechars(display_json(self.request_headers, indent=0), 50)
    short_request_headers.short_description = _('request headers')
    short_request_headers.filter_by = 'request_headers'

    def response_time(self):
        return (self.response_timestamp - self.request_timestamp).total_seconds() if self.response_timestamp else None
    response_time.short_description = _('Response time')

    def short_path(self):
        return truncatechars(self.path, 50)
    short_path.short_description = _('Path')
    short_path.filter_by = 'path'
    short_path.order_by = 'path'

    def short_response_body(self):
        return truncatechars(self.response_body, 50)
    short_response_body.short_description = _('response body')
    short_response_body.filter_by = 'response_body'

    def short_request_body(self):
        return truncatechars(self.request_body, 50)
    short_request_body.short_description = _('request body')
    short_request_body.filter_by = 'request_body'

    def __str__(self):
        return ' '.join(
            (force_text(v) for v in (
                self.slug, self.response_code, localtime(self.request_timestamp.replace(microsecond=0))
            ) if v)
        )

    class Meta:
        abstract = True

    class UIMeta:
        default_ui_filter_by = 'id'


class InputLoggedRequest(LoggedRequest):

    user = models.ForeignKey(django_settings.AUTH_USER_MODEL, verbose_name=_('user'), null=True, blank=True,
                             on_delete=models.SET_NULL)
    ip = models.GenericIPAddressField(_('IP address'), null=False, blank=False)
    type = NumEnumField(verbose_name=_('type'), enum=InputLoggedRequestType,
                        default=InputLoggedRequestType.COMMON_REQUEST,
                        null=False, blank=False)

    objects = InputLoggedRequestManager()

    def update_from_response(self, response):
        self.response_timestamp = timezone.now()
        self.status = self.get_status(response.status_code)
        self.response_code = response.status_code
        self.response_headers = clean_headers(dict(response.items()))

        if (not response.streaming and settings.LOG_RESPONSE_BODY_CONTENT_TYPES is not None and
                response.get('content-type', '').split(';')[0] in settings.LOG_RESPONSE_BODY_CONTENT_TYPES):
            response_body = clean_body(response.content, settings.LOG_RESPONSE_BODY_LENGTH)
        else:
            response_body = ''

        self.response_body = response_body

    class Meta:
        verbose_name = _('input logged request')
        verbose_name_plural = _('input logged requests')
        ordering = ('-created_at',)

    class SmartMeta:
        is_cleaned_pre_save = False


class OutputLoggedRequest(LoggedRequest):

    input_logged_request = models.ForeignKey(InputLoggedRequest, verbose_name=_('input logged request'), null=True,
                                             blank=True, on_delete=models.SET_NULL,
                                             related_name='output_logged_requests')

    class Meta:
        verbose_name = _('output logged request')
        verbose_name_plural = _('output logged requests')
        ordering = ('-created_at',)

    class SmartMeta:
        is_cleaned_pre_save = False


class OutputLoggedRequestRelatedObjects(models.Model):

    output_logged_request = models.ForeignKey(OutputLoggedRequest, verbose_name=_('output logged requests'), null=False,
                                              blank=False, related_name='related_objects', on_delete=models.CASCADE)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')

    def display_object(self, request):
        from is_core.utils import render_model_object_with_link

        return render_model_object_with_link(request, self.content_object) if self.content_object else None
    display_object.short_description = _('object')


class CommandLog(SmartModel):
    """
    Represents a log of a command run.

    Attributes:
        start: Date and time when command was started.
        stop: Date and time when command finished.
        name: Name of the command.
        options: Arguments/options the command was run with.
        executed_from_command_line: Flag that indicates if command was run from the command line.
        output: Standard and error output of the command.
        is_successful: Flag that indicates if command finished successfully.
    """
    start = models.DateTimeField(blank=False, null=False, editable=False, verbose_name=_('start'))
    stop = models.DateTimeField(blank=True, null=True, editable=False, verbose_name=_('stop'))
    name = models.CharField(max_length=250, blank=False, null=False, editable=False, db_index=True,
                            verbose_name=_('name'))
    input = models.TextField(blank=False, null=False, editable=False, verbose_name=_('input'))
    executed_from_command_line = models.BooleanField(blank=False, null=False, default=False, editable=False,
                                                     verbose_name=_('executed from command line'))
    output = models.TextField(blank=True, null=True, editable=False, verbose_name=_('output'))
    is_successful = models.BooleanField(blank=False, null=False, default=False, editable=False,
                                        verbose_name=_('finished successfully'))
    error_message = models.TextField(verbose_name=_('error message'), null=True, blank=True, editable=False)

    class Meta:
        verbose_name = _('command log')
        verbose_name_plural = _('command logs')
        ordering = ('-created_at',)

    class SmartMeta:
        is_cleaned_pre_save = False

    class UIMeta:
        default_ui_filter_by = 'id'


class CeleryTaskManager(models.Manager):

    def filter_stale(self, **kwargs):
        return self.filter(
            state__in={CeleryTaskLogState.WAITING, CeleryTaskLogState.ACTIVE, CeleryTaskLogState.RETRIED},
            expires__gt=timezone.now()
        )


class CeleryTaskLog(SmartModel):

    objects = CeleryTaskManager()

    start = models.DateTimeField(verbose_name=_('start'), null=True, blank=True)
    stop = models.DateTimeField(verbose_name=_('stop'), null=True, blank=True)
    name = models.CharField(verbose_name=_('task name'), null=False, blank=False, max_length=250)
    state = NumEnumField(verbose_name=_('state'), null=False, blank=False, enum=CeleryTaskLogState,
                         default=CeleryTaskLogState.WAITING)
    error_message = models.TextField(verbose_name=_('error message'), null=True, blank=True)
    queue_name = models.CharField(verbose_name=_('queue name'), null=True, blank=True, max_length=250)
    input = models.TextField(blank=True, null=True, editable=False, verbose_name=_('input'))
    output = models.TextField(blank=True, null=True, editable=False, verbose_name=_('output'))
    retries = models.PositiveSmallIntegerField(verbose_name=_('retries'), null=False, blank=False, default=0)
    estimated_time_of_arrival = models.DateTimeField(verbose_name=_('estimated time of arrival'), null=False,
                                                     blank=False)
    expires = models.DateTimeField(verbose_name=_('estimated time of arrival'), null=True, blank=True)

    class Meta:
        verbose_name = _('celery task')
        verbose_name_plural = _('celery tasks')
        ordering = ('-created_at',)

    class SmartMeta:
        is_cleaned_pre_save = False

    class UIMeta:
        default_ui_filter_by = 'id'

    def __str__(self):
        return '{} ({})'.format(self.name, self.get_state_display())
