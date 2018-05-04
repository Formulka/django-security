from datetime import timedelta

from io import StringIO

from django.contrib.auth.models import User
from django.core.management import call_command
from django.utils import timezone

from germanium.test_cases.default import GermaniumTestCase
from germanium.tools import assert_equal, assert_false

from auth_token.models import Token
from auth_token.config import settings


__all__ = (
   'CleanTokensCommandTestCase',
)


class CleanTokensCommandTestCase(GermaniumTestCase):

    def test_clean_tokens_remove_only_old_tokens(self):
        user = User.objects._create_user('test', 'test@test.cz', 'test', is_staff=False, is_superuser=False)
        expired_tokens = [Token.objects.create(user=user, ip='127.0.0.1') for _ in range(10)]
        not_expired_tokens = [Token.objects.create(user=user, ip='127.0.0.1') for _ in
                              range(settings.COUNT_USER_PRESERVED_TOKENS - 5)]
        Token.objects.filter(pk__in=[token.pk for token in expired_tokens]).update(
            last_access=timezone.now() - timedelta(seconds=settings.MAX_TOKEN_AGE))
        call_command('clean_tokens', stdout=StringIO(), stderr=StringIO())
        assert_equal(Token.objects.filter(pk__in=[token.pk for token in not_expired_tokens]).count(),
                     settings.COUNT_USER_PRESERVED_TOKENS - 5)
        assert_equal(Token.objects.filter(pk__in=[token.pk for token in expired_tokens]).count(), 5)
