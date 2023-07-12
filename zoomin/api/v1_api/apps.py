"""Module from Django app template."""
from django.apps import AppConfig


class V1ApiConfig(AppConfig):
    """Class from Django app template"""

    default_auto_field = "django.db.models.BigAutoField"
    name = "zoomin.api.v1_api"
