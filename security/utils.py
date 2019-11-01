import atexit
import gzip
import json
import os
import re
import sys
import traceback
from datetime import datetime, time, timedelta
from importlib import import_module
from io import StringIO

from six.moves import input

from django.core import serializers
from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand
from django.core.serializers.json import DjangoJSONEncoder
from django.urls import resolve
from django.urls.exceptions import Resolver404
from django.utils import timezone
from django.utils.encoding import force_bytes, force_text
from django.utils.module_loading import import_string
from django.utils.timezone import now, utc

from .config import settings


UNIT_OPTIONS = {
    'h': lambda amount: timezone.now() - timedelta(hours=amount),
    'd': lambda amount: timezone.now() - timedelta(days=amount),
    'w': lambda amount: timezone.now() - timedelta(weeks=amount),
    'm': lambda amount: timezone.now() - timedelta(days=(30 * amount)),  # 30-day month
    'y': lambda amount: timezone.now() - timedelta(weeks=(52 * amount)),  # 364-day year
}

storage = import_string(settings.BACKUP_STORAGE_CLASS)()


def is_base_collection(v):
    return isinstance(v, (list, tuple, set))


def get_throttling_validators(name):
    try:
        return getattr(import_module(settings.DEFAULT_THROTTLING_VALIDATORS_PATH), name)
    except (ImportError, AttributeError):
        raise ImproperlyConfigured('Throttling validator configuration {} is not defined'.format(name))


def get_headers(request):
    regex = re.compile('^HTTP_')
    return dict((regex.sub('', header), value) for (header, value)
                in request.META.items() if header.startswith('HTTP_'))


def remove_nul_from_string(value):
    return value.replace('\x00', '')


class LogStringIO(StringIO):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_newline = 0

    def write(self, s):
        line_first = True
        for line in s.split('\n'):
            if not line_first:
                super().write('\n')
                self._last_newline = self.tell()
            cursor_first = True
            for cursor_block in line.split('\r'):
                if not cursor_first:
                    self.seek(self._last_newline)
                    self.truncate()
                cursor_first = False
                super().write(cursor_block)
            line_first = False

    def isatty(self):
        return True


class TeeStringIO:
    """
    StringIO that additionally writes to another stream(s).
    """
    ANSI_ESCAPE_REGEX = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')

    def __init__(self, out, log_stream):
        self._out = out
        self._log_stream = log_stream
        self._last_newline = 0
        super().__init__()

    def write(self, s):
        if self.isatty():
            self._out.write(s)
        else:
            self._out.write(self.ANSI_ESCAPE_REGEX.sub('', s))
        self._log_stream.write(s)

    def __getattr__(self, name):
        return getattr(self._out, name)

    def isatty(self):
        return hasattr(self._out, 'isatty') and self._out.isatty()

    def flush(self):
        self._out.flush()
        self._log_stream.flush()


class CommandLogger(StringIO):
    """
    A helper class that runs a django command and logs details about its run into DB.
    """

    def __init__(self, command_function, command_args=None, command_kwargs=None, stdout=None, stderr=None, **kwargs):
        """
        Initializes the command logger.

        Arguments:
            command_function: Callable that implements the command.
            stdout: Custom stream where command's standard output will be written.
            stderr: Custom stream where command's error output will be written.
            **kwargs: Keyword arguments passed to CommandLog model.
        """
        assert 'name' in kwargs, 'Key name must be provided in kwargs'

        self.command_function = command_function
        self.command_args = command_args or ()
        self.command_kwargs = command_kwargs or {}

        self.kwargs = kwargs
        self.stdout = stdout if stdout else sys.stdout
        self.stderr = stderr if stderr else sys.stderr

    def run(self):
        """
        Runs the command function and returns its return value or re-raises any exceptions. The run of the command will
        not be logged if it is in excluded commands setting.
        """
        from security.models import CommandLog

        if self.kwargs['name'] in settings.COMMAND_LOG_EXCLUDED_COMMANDS:
            return self.command_function(
                stdout=self.stdout, stderr=self.stderr, *self.command_args, **self.command_kwargs
            )

        self.output = LogStringIO()
        stdout = TeeStringIO(self.stdout, self.output)
        stderr = TeeStringIO(self.stderr, self.output)

        self.command_log = CommandLog.objects.create(start=now(), **self.kwargs)

        # register call of the finish method in case the command exits the interpreter prematurely
        atexit.register(lambda: self._finish(error_message='Command was killed'))

        try:
            ret_val = self.command_function(
                stdout=stdout, stderr=stderr, *self.command_args, **self.command_kwargs
            )
            self._finish(success=True)
            return ret_val
        except Exception as ex:  # pylint: disable=W0703
            self._finish(success=False, error_message=traceback.format_exc())
            raise ex

    def _finish(self, success=False, error_message=None):
        if self.command_log.stop is None:
            self.command_log.change_and_save(
                output=self.output.getvalue(),
                stop=now(),
                is_successful=success,
                error_message=error_message
            )


class PurgeLogsBaseCommand(BaseCommand):

    help = ""
    args = '[amount duration]'

    model = None
    timestamp_field = None

    def get_timestamp_field(self):
        return self.timestamp_field

    def get_model(self):
        return self.model

    def add_arguments(self, parser):
        parser.add_argument('--expiration', action='store', dest='expiration',
                            help='Sets the timedelta from which logs will be removed.', required=True)
        parser.add_argument('--noinput', action='store_false', dest='interactive', default=True,
                            help='Tells Django to NOT prompt the user for input of any kind.')
        parser.add_argument('--backup', action='store', dest='backup', default=False,
                            help='Tells Django where to backup removed logs.')

    def serialize(self, qs):
        data = serializers.serialize('python', qs)

        for obj_data in data:
            del obj_data['pk']
        return data

    def backup_logs_and_clean_data(self, qs, backup_path):
        for timestamp in qs.datetimes(self.get_timestamp_field(), 'day', tzinfo=utc):
            min_timestamp = datetime.combine(timestamp, time.min).replace(tzinfo=utc)
            max_timestamp = datetime.combine(timestamp, time.max).replace(tzinfo=utc)
            qs_filtered_by_day = qs.filter(
                **{'{}__range'.format(self.get_timestamp_field()): (min_timestamp, max_timestamp)}
            )
            if backup_path:
                log_file_path = os.path.join(backup_path, force_text(timestamp.date()))

                if storage.exists('{}.json.zip'.format(log_file_path)):
                    i = 0
                    while storage.exists('{}({}).json.zip'.format(log_file_path, i)):
                        i += 1
                    log_file_path = '{}({})'.format(log_file_path, i)

                self.stdout.write(4 * ' ' + 'generate backup file: ' + log_file_path)
                storage.save(
                    '{}.json.zip'.format(log_file_path),
                    ContentFile(gzip.compress(
                        force_bytes(json.dumps(self.serialize(qs_filtered_by_day), cls=DjangoJSONEncoder, indent=5)),
                    )),
                )
            self.stdout.write(4 * ' ' + 'clean logs for day {}'.format(timestamp.date()))
            qs_filtered_by_day.delete()

    def handle(self, expiration, **options):
        # Check we have the correct values
        unit = expiration[-1]
        amount = expiration[0:-1]
        try:
            amount = int(amount)
        except ValueError:
            raise CommandError('Invalid expiration format')

        if unit not in UNIT_OPTIONS:
            raise CommandError('Invalid expiration format')

        model = self.get_model()
        qs = model.objects.filter(**{'{}__lte'.format(self.get_timestamp_field()): UNIT_OPTIONS[unit](amount)})

        if qs.count() == 0:
            self.stdout.write('There are no logs to delete.')
        else:
            if options.get('interactive'):
                confirm = input('''
                You have requested a database reset.
                This will IRREVERSIBLY DESTROY any
                logs created before {} {}
                ago. That is a total of {} logs.
                Are you sure you want to do this?
                Type 'yes' to continue, or 'no' to cancel: '''.format(amount, unit, qs.count()))
            else:
                confirm = 'yes'

            if confirm == 'yes':
                try:
                    self.stdout.write('Clean data')
                    self.backup_logs_and_clean_data(qs, options.get('backup'))
                except IOError as ex:
                    self.stderr.write(force_text(ex))


def regex_sub_groups_global(pattern, repl, string, flags=0):
    """
    Globally replace all groups inside pattern with `repl`.
    If `pattern` doesn't have groups the whole match is replaced.
    """
    for search in reversed(list(re.finditer(pattern, string, flags))):
        for i in range(len(search.groups()), 0 if search.groups() else -1, -1):
            start, end = search.span(i)
            string = string[:start] + repl + string[end:]
    return string


def get_view_from_request_or_none(request):
    try:
        return resolve(request.path).func
    except Resolver404:
        return None
