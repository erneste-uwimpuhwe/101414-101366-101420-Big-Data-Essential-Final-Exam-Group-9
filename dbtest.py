import django
from django.conf import settings

settings.configure(
    DATABASES={"default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "ridehail", "USER": "root", "PASSWORD": "",
        "HOST": "127.0.0.1", "PORT": "3306",
    }},
    INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth"],
)
django.setup()

from django.db import connection

with connection.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM trips")
    print("Django version:", django.get_version())
    print("rows in trips:", cur.fetchone()[0])
