"""Django URL module."""
from django.urls import include, path


from rest_framework import routers
from zoomin.api.v1_api.views import (
    RegionDataViewSet,
    VariableDataViewSet,
    TagsViewSet,
    RegionMetadataViewSet,
    VariableMetadataViewSet,
    RegionDataMiniVersionViewSet,
)

router = routers.DefaultRouter()

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path("region_metadata/", RegionMetadataViewSet.as_view(), name="region_list"),
    path("region_data/", RegionDataViewSet.as_view(), name="region_data"),
    path(
        "region_data/mini_version/",
        RegionDataMiniVersionViewSet.as_view(),
        name="region_data_mini",
    ),
    path("variable_metadata/", VariableMetadataViewSet.as_view(), name="var_metadata"),
    path("variable_data/", VariableDataViewSet.as_view(), name="var_data"),
    path("tag_list/", TagsViewSet.as_view(), name="tags"),
    path("api-auth/", include("rest_framework.urls", namespace="rest_framework")),
]
