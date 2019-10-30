import os
import base64
import logging
import pickle
import sys

from datetime import timedelta

from django.conf import settings
from django.core.management import call_command
from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.utils.timezone import now

try:
    from celery import Task
    from celery import shared_task
    from celery.exceptions import CeleryError
except ImportError:
    raise ImproperlyConfigured('Missing celery library, please install it')

from .models import CeleryTaskLog, CeleryTaskLogState
from .utils import LogStringIO


LOGGER = logging.getLogger(__name__)


class LoggedTask(Task):

    abstract = True
    logger_level = logging.WARNING
    retry_error_message = (
        'Task "{task_name}" ({task}) failed on exception: "{exception}", attempt: "{attempt}" and will be retried'
    )
    fail_error_message = 'Task "{task_name}" ({task}) failed on exception: "{exception}", attempt: "{attempt}"'
    stale_time_limit = None
    # Support set retry delay in list. Retry countdown value is get from list where index is attempt
    # number (request.retries)
    default_retry_delays = None

    def get_task(self, task_id=None):
        return CeleryTaskLog.objects.get(pk=self.request.id if task_id is None else task_id)

    def __call__(self, *args, **kwargs):
        """
        Overrides parent which works with thread stack. We didn't want to allow change context which was generated in
        one of apply methods. Call task directly is now disallowed.
        """
        req = self.request_stack.top

        if not req or req.called_directly:
            raise CeleryError(
                'Task cannot be called directly. Please use apply, apply_async or apply_async_on_commit methods'
            )

        if req._protected:
            raise CeleryError('Request is protected')
        # request is protected (no usage in celery but get from function _install_stack_protection in
        # celery library)
        req._protected = 1

        # Every set attr is sent here
        self.request.output_stream = LogStringIO()
        self.on_start(self.request.id, args, kwargs)
        return self.run(*args, **kwargs)

    def _call_callback(self, event, *args, **kwargs):
        if hasattr(self, 'on_{}_callback'.format(event)):
            getattr(self, 'on_{}_callback'.format(event))(*args, **kwargs)

    def on_apply(self, task_id, args, kwargs):
        self._call_callback('apply', task_id, args, kwargs)

    def on_start(self, task_id, args, kwargs):
        self.get_task().change_and_save(state=CeleryTaskLogState.ACTIVE, start=now())
        self._call_callback('start', task_id, args, kwargs)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        task = self.get_task()
        LOGGER.log(self.logger_level, self.retry_error_message.format(
            attempt=self.request.retries, exception=str(exc), task=task, task_name=task.name, **kwargs
        ))
        task.change_and_save(
            state=CeleryTaskLogState.RETRIED,
            error_message=einfo,
            output=self.request.output_stream.getvalue(),
            retries=self.request.retries
        )

    def expire_task(self, task):
        task.change_and_save(
            state=CeleryTaskLogState.EXPIRED,
            stop=now(),
            error_message='Task execution was expired by command'
        )

    def on_success(self, retval, task_id, args, kwargs):
        if retval:
            self.request.output_stream.write('Return value is "{}"'.format(retval))

        self.get_task().change_and_save(
            state=CeleryTaskLogState.SUCCEEDED,
            stop=now(),
            output=self.request.output_stream.getvalue(),
            retries=self.request.retries
        )
        self._call_callback('success', task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        try:
            task = self.get_task()
            LOGGER.log(self.logger_level, self.retry_error_message.format(
                attempt=self.request.retries, exception=str(exc), task=task, task_name=task.name, **kwargs
            ))
            task.change_and_save(
                state=CeleryTaskLogState.FAILED,
                stop=now(),
                error_message=einfo,
                output=self.request.output_stream.getvalue(),
                retries=self.request.retries
            )
        except CeleryTaskLog.DoesNotExist:
            pass
        self._call_callback('failure', task_id, args, kwargs)

    def _get_eta(self, options):
        if options.get('countdown') is not None:
            return now() + timedelta(seconds=options['countdown'])
        elif options.get('eta'):
            return options['eta']
        else:
            return now()

    def _get_time_limit(self, options):
        if options.get('time_limit') is not None:
            return options['time_limit']
        elif self.soft_time_limit is not None:
            return self.soft_time_limit
        else:
            return settings.CELERYD_TASK_SOFT_TIME_LIMIT

    def _get_stale_time_limit(self, options):
        if options.get('stale_time_limit') is not None:
            return options['stale_time_limit']
        elif self.stale_time_limit is not None:
            return self.stale_time_limit
        else:
            return settings.CELERYD_TASK_STALE_TIME_LIMIT

    def _get_expires(self, options):
        if options.get('expires') is not None:
            return options['expires']
        elif self.expires is not None:
            return self.expires
        elif hasattr(settings, 'CELERYD_TASK_STALE_TIME_LIMIT'):
            return now() + timedelta(
                seconds=(self._get_stale_time_limit(options) * 60) - self._get_time_limit(options)
            )
        else:
            return None

    def _create_task(self, options, task_args, task_kwargs):
        task_input = []
        if task_args:
            task_input += [str(v) for v in task_args]
        if task_kwargs:
            task_input += ['{}={}'.format(k, v) for k, v in task_kwargs.items()]

        task = CeleryTaskLog.objects.create(
            name=self.name,
            state=CeleryTaskLogState.WAITING,
            queue_name=options.get('queue', getattr(self, 'queue', settings.CELERY_DEFAULT_QUEUE)),
            input=', '.join(task_input),
            estimated_time_of_arrival=options['eta'],
            expires=options['expires']
        )
        return str(task.pk)

    def _update_options(self, options):
        options['eta'] = self._get_eta(options)
        options['expires'] = self._get_expires(options)
        options.pop('countdown', None)
        options.pop('stale_time_limit', None)
        return options

    def apply_async_on_commit(self, args=None, kwargs=None, **options):
        if sys.argv[1:2] == ['test']:
            self.apply_async(args=args, kwargs=kwargs, **options)
        else:
            self_inst = self
            transaction.on_commit(
                lambda: self_inst.apply_async(args=args, kwargs=kwargs, **options)
            )

    def apply(self, args=None, kwargs=None, **options):
        options = self._update_options(options)
        if 'task_id' in options:
            # For working celery retry
            self.get_task(options['task_id']).change_and_save(
                estimated_time_of_arrival=options['eta'],
                expires=options['expires']
            )
            return super().apply(args=args, kwargs=kwargs, **options)
        else:
            task_id = self._create_task(options, args, kwargs)
            self.on_apply(task_id, args, kwargs)

            return super().apply(args=args, kwargs=kwargs, task_id=task_id, **options)

    def apply_async(self, args=None, kwargs=None, **options):
        app = self._get_app()
        if app.conf.task_always_eager:
            # Is called apply
            if not 'task_id' in options:
                options = self._update_options(options)
                options['task_id'] = self._create_task(options, args, kwargs)
            return super().apply_async(args=args, kwargs=kwargs, **options)

        else:
            options = self._update_options(options)

            if 'task_id' in options:
                # For working celery retry
                self.get_task(options['task_id']).change_and_save(
                    estimated_time_of_arrival=options['eta'],
                    expires=options['expires']
                )
                return super().apply_async(args=args, kwargs=kwargs, **options)
            else:
                task_id = self._create_task(options, args, kwargs)
                self.on_apply(task_id, args, kwargs)
                return super().apply_async(args=args, kwargs=kwargs, task_id=task_id, **options)

    def retry(self, args=None, kwargs=None, exc=None, throw=True,
              eta=None, countdown=None, max_retries=None, default_retry_delays=None, **options):
        if (default_retry_delays or (
                max_retries is None and eta is None and countdown is None and max_retries is None
                and self.default_retry_delays)):
            default_retry_delays = self.default_retry_delays if default_retry_delays is None else default_retry_delays
            max_retries = len(default_retry_delays)
            countdown = default_retry_delays[self.request.retries] if self.request.retries < max_retries else None
        return super().retry(
            args=args, kwargs=kwargs, exc=exc, throw=throw,
            eta=eta, countdown=countdown, max_retries=max_retries, **options
        )


def obj_to_string(obj):
    return base64.encodebytes(pickle.dumps(obj)).decode('utf8')


def string_to_obj(obj_string):
    return pickle.loads(base64.decodebytes(obj_string.encode('utf8')))


@shared_task(
    base=LoggedTask,
    bind=True,
    name='call_django_command'
)
def call_django_command(self, command_name, command_args=None):
    command_args = [] if command_args is None else command_args
    call_command(
        command_name,
        settings=os.environ.get('DJANGO_SETTINGS_MODULE'),
        *command_args,
        stdout=self.request.output_stream,
        stderr=self.request.output_stream,
    )
