"""api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
"""
from django.contrib import admin
from django.urls import path, include
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from rest_framework import permissions

SchemaView = get_schema_view(
    openapi.Info(
        title="Zoomin API",
        default_version="v1",
        description="An API for the data delivery",
        terms_of_service="https://www.zoomin.com/policies/terms/",
        contact=openapi.Contact(email="s.patil@fz-juelich.de"),
        license=openapi.License(name="BSD License"),
    ),
    public=True,
    permission_classes=[permissions.AllowAny],
)

urlpatterns = [
    path("admin/", admin.site.urls),
    path(
        "api/v1/docs/",
        SchemaView.with_ui("swagger", cache_timeout=0),
        name="schema-swagger-ui",
    ),
    path("api/v1/", include("v1_api.urls")),
    path("debug/", include("debug_toolbar.urls")),
]
