"""Django views module."""
import os
import datetime
from typing import Any
from django.db.models import Q, Prefetch, QuerySet
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import generics
from rest_framework.serializers import ModelSerializer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from zoomin.api.v1_api import serializers
from zoomin.api.v1_api import models

API_KEY = os.environ.get("DJANGO_API_Key")
API_KEY_EXT = os.environ.get("DJANGO_API_Key_ext")


class RegionMetadataViewSet(generics.ListAPIView):
    """List of all regions or a specified region, at a specified spatial resolution and for a specified country."""

    # only get method is allowed
    http_method_names = ["get"]

    def get_queryset(self) -> QuerySet:
        """Filter data based on the query."""
        query_api_key = self.request.query_params.get("api_key")
        query_resolution = self.request.query_params.get("resolution")
        query_region = self.request.query_params.get("region", None)
        query_country = self.request.query_params.get("country")

        # reveal data only if API_KEY matches
        if query_api_key in [API_KEY, API_KEY_EXT]:
            # log external queries
            if query_api_key == API_KEY_EXT:
                curr_time = datetime.datetime.now()

                log = f"""query_time - {curr_time}\nquery_resolution - {query_resolution}\nquery_region - {query_region}\nquery_country - {query_country}"""

                save_path = os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "data",
                    "output",
                    "query_logs",
                    f"region_metadata_{curr_time.timestamp()}.log",
                )

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(log)

            if query_region is None:
                region_filter = (
                    Q(parent_region_code__startswith=query_country)
                    | Q(region_code=query_country)
                ) & Q(resolution=query_resolution)
            else:
                region_filter = (
                    (
                        Q(parent_region_code__startswith=query_country)
                        | Q(region_code=query_country)
                    )
                    & Q(region_code=query_region)
                    & Q(resolution=query_resolution)
                )

            queryset = models.Regions.objects.filter(region_filter).select_related(
                "citation"
            )

            return queryset

        raise ValidationError(detail="Invalid API key")

    def get_serializer_class(self) -> ModelSerializer:
        """Get serialized data corresponding to the query."""

        serializer = serializers.RegionsSerializer

        return serializer

    # swagger documentation
    api_key_param = openapi.Parameter(
        "api_key",
        in_=openapi.IN_QUERY,
        description="The super-secret API key",
        required=True,
        type=openapi.TYPE_STRING,
    )

    resolution_param = openapi.Parameter(
        "resolution",
        in_=openapi.IN_QUERY,
        description="Data resolution - NUTS1, NUTS2, NUTS3, or LAU",
        required=True,
        type=openapi.TYPE_STRING,
    )

    region_param = openapi.Parameter(
        "region",
        in_=openapi.IN_QUERY,
        description="Specify a desired region's code",
        required=False,
        type=openapi.TYPE_STRING,
    )

    country_param = openapi.Parameter(
        "country",
        in_=openapi.IN_QUERY,
        description="Specify the country of the desired region",
        required=False,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(
        manual_parameters=[
            resolution_param,
            api_key_param,
            region_param,
            country_param,
        ]
    )
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """Add swagger documentation."""
        response = super().get(request, *args, **kwargs)
        return response


class RegionDataViewSet(generics.ListAPIView):
    """All the data belonging to a single region."""

    # only get method is allowed
    http_method_names = ["get"]

    def get_queryset(self) -> QuerySet:
        """Filter data based on the query."""
        query_api_key = self.request.query_params.get("api_key")
        query_resolution = self.request.query_params.get("resolution")
        query_region = self.request.query_params.get("region")
        query_country = self.request.query_params.get("country")

        # reveal data only if API_KEY matches
        if query_api_key in [API_KEY, API_KEY_EXT]:
            # log external queries
            if query_api_key == API_KEY_EXT:
                curr_time = datetime.datetime.now()

                log = f"""query_time - {curr_time}\nquery_resolution - {query_resolution}\nquery_region - {query_region}\nquery_country - {query_country}"""

                save_path = os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "data",
                    "output",
                    "query_logs",
                    f"region_data_{curr_time.timestamp()}.log",
                )

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(log)

            ## region filter
            proxy_metric_prefetch = Prefetch(
                "var_detail__proxy_metrics",
                queryset=models.ProxyMetrics.objects.filter(
                    region__region_code__startswith=query_country
                ).filter(region__resolution="NUTS0"),
            )

            region_filter = (
                (
                    Q(region__parent_region_code__startswith=query_country)
                    | Q(region__region_code=query_country)
                )
                & Q(region__region_code=query_region)
                & Q(region__resolution=query_resolution)
            )

            queryset = (
                models.RegionData.objects.select_related(
                    "var_detail",
                    "pathway",
                    "citation",
                    "disaggregation_method",
                    "climate_experiment",
                    "original_resolution",
                )
                .filter(chosen=1)
                .filter(region_filter)
                .prefetch_related("var_detail__taggings")
                .prefetch_related(proxy_metric_prefetch)
            )

            return queryset

        raise ValidationError(detail="Invalid API key")

    def get_serializer_class(self) -> ModelSerializer:
        """Get serialized data corresponding to the query."""

        serializer = serializers.SingleRegionAllDataSerializer

        return serializer

    # swagger documentation
    api_key_param = openapi.Parameter(
        "api_key",
        in_=openapi.IN_QUERY,
        description="The super-secret API key",
        required=True,
        type=openapi.TYPE_STRING,
    )

    resolution_param = openapi.Parameter(
        "resolution",
        in_=openapi.IN_QUERY,
        description="Data resolution - NUTS0, NUTS1, NUTS2, NUTS3, or LAU",
        required=True,
        type=openapi.TYPE_STRING,
    )

    region_param = openapi.Parameter(
        "region",
        in_=openapi.IN_QUERY,
        description="Specify a desired region's code",
        required=False,
        type=openapi.TYPE_STRING,
    )

    country_param = openapi.Parameter(
        "country",
        in_=openapi.IN_QUERY,
        description="Specify the country of the desired region",
        required=False,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(
        manual_parameters=[resolution_param, api_key_param, region_param, country_param]
    )
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """Add swagger documentation."""
        response = super().get(request, *args, **kwargs)
        return response


class RegionDataMiniVersionViewSet(generics.ListAPIView):
    """All the data belonging to a single region, but fewer fields than RegionDataViewSet."""

    # only get method is allowed
    http_method_names = ["get"]

    def get_queryset(self) -> QuerySet:
        """Filter data based on the query."""
        query_api_key = self.request.query_params.get("api_key")
        query_resolution = self.request.query_params.get("resolution")
        query_region = self.request.query_params.get("region")
        query_country = self.request.query_params.get("country")

        # reveal data only if API_KEY matches
        if query_api_key in [API_KEY, API_KEY_EXT]:
            # log external queries
            if query_api_key == API_KEY_EXT:
                curr_time = datetime.datetime.now()

                log = f"""query_time - {curr_time}\nquery_resolution - {query_resolution}\nquery_region - {query_region}\nquery_country - {query_country}"""

                save_path = os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "data",
                    "output",
                    "query_logs",
                    f"region_data_mini_{curr_time.timestamp()}.log",
                )

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(log)

            region_filter = (
                (
                    Q(region__parent_region_code__startswith=query_country)
                    | Q(region__region_code=query_country)
                )
                & Q(region__region_code=query_region)
                & Q(region__resolution=query_resolution)
            )

            queryset = (
                models.RegionData.objects.select_related(
                    "var_detail",
                    "pathway",
                    "climate_experiment",
                )
                .filter(chosen=1)
                .filter(region_filter)
                .prefetch_related("var_detail__taggings")
            )

            return queryset

        raise ValidationError(detail="Invalid API key")

    def get_serializer_class(self) -> ModelSerializer:
        """Get serialized data corresponding to the query."""

        serializer = serializers.SingleRegionAllDataMiniVersionSerializer

        return serializer

    # swagger documentation
    api_key_param = openapi.Parameter(
        "api_key",
        in_=openapi.IN_QUERY,
        description="The super-secret API key",
        required=True,
        type=openapi.TYPE_STRING,
    )

    resolution_param = openapi.Parameter(
        "resolution",
        in_=openapi.IN_QUERY,
        description="Data resolution - NUTS0, NUTS1, NUTS2, NUTS3, or LAU",
        required=True,
        type=openapi.TYPE_STRING,
    )

    region_param = openapi.Parameter(
        "region",
        in_=openapi.IN_QUERY,
        description="Specify a desired region's code",
        required=False,
        type=openapi.TYPE_STRING,
    )

    country_param = openapi.Parameter(
        "country",
        in_=openapi.IN_QUERY,
        description="Specify the country of the desired region",
        required=False,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(
        manual_parameters=[resolution_param, api_key_param, region_param, country_param]
    )
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """Add swagger documentation."""
        response = super().get(request, *args, **kwargs)
        return response


# class VariableListViewSet(generics.ListAPIView): TODO: currently this is not possible due to the db structure.  perhaps in the future.
#     """API view of a list of all variables that are available for a specified country."""


class VariableDataViewSet(generics.ListAPIView):
    """Single variable data for all regions at a specified resolution, in a specified country."""

    # only get method is allowed
    http_method_names = ["get"]

    def get_queryset(self) -> QuerySet:
        """Filter data based on the query."""
        query_api_key = self.request.query_params.get("api_key")
        query_variable = self.request.query_params.get("variable")
        query_resolution = self.request.query_params.get("resolution")
        query_country = self.request.query_params.get("country")

        # reveal data only if API_KEY matches
        if query_api_key in [API_KEY, API_KEY_EXT]:
            # log external queries
            if query_api_key == API_KEY_EXT:
                curr_time = datetime.datetime.now()

                log = f"""query_time - {curr_time}\nquery_resolution - {query_resolution}\nquery_country - {query_country}\nquery_variable - {query_variable}"""

                save_path = os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "data",
                    "output",
                    "query_logs",
                    f"variable_data_{curr_time.timestamp()}.log",
                )

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(log)

            # filter on data
            region_filter = (
                Q(region__parent_region_code__startswith=query_country)
                | Q(region__region_code=query_country)
            ) & Q(region__resolution=query_resolution)

            queryset = (
                models.RegionData.objects.select_related(
                    "region",
                    "pathway",
                    "citation",
                    "disaggregation_method",
                    "climate_experiment",
                    "original_resolution",
                )
                .filter(chosen=1)
                .filter(var_detail__var_name=query_variable)
                .filter(region_filter)
            )

            return queryset

        raise ValidationError(detail="Invalid API key")

    def get_serializer_class(self) -> ModelSerializer:
        """Get serialized data corresponding to the query."""
        serializer = serializers.SingleVarRegionDataSerializer

        return serializer

    # swagger documentation
    api_key_param = openapi.Parameter(
        "api_key",
        in_=openapi.IN_QUERY,
        description="The super-secret API key",
        required=True,
        type=openapi.TYPE_STRING,
    )

    variable_param = openapi.Parameter(
        "variable",
        in_=openapi.IN_QUERY,
        description="Specify a desired variable",
        required=True,
        type=openapi.TYPE_STRING,
    )

    resolution_param = openapi.Parameter(
        "resolution",
        in_=openapi.IN_QUERY,
        description="Data resolution - NUTS0, NUTS1, NUTS2, NUTS3, or LAU",
        required=True,
        type=openapi.TYPE_STRING,
    )

    country_param = openapi.Parameter(
        "country",
        in_=openapi.IN_QUERY,
        description="Specify the country of the desired region",
        required=True,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(
        manual_parameters=[
            api_key_param,
            variable_param,
            resolution_param,
            country_param,
        ]
    )
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """Add swagger documentation."""
        response = super().get(request, *args, **kwargs)
        return response


class VariableMetadataViewSet(generics.ListAPIView):
    """Metadata of a single variable for a specified country."""

    # only get method is allowed
    http_method_names = ["get"]

    def get_queryset(self) -> QuerySet:
        """Filter data based on the query."""
        query_api_key = self.request.query_params.get("api_key")
        query_variable = self.request.query_params.get("variable")
        query_country = self.request.query_params.get("country")

        # reveal data only if API_KEY matches
        if query_api_key in [API_KEY, API_KEY_EXT]:
            # log external queries
            if query_api_key == API_KEY_EXT:
                curr_time = datetime.datetime.now()

                log = f"""query_time - {curr_time}\nquery_country - {query_country}\nquery_variable - {query_variable}"""

                save_path = os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "..",
                    "..",
                    "data",
                    "output",
                    "query_logs",
                    f"variable_metadata_{curr_time.timestamp()}.log",
                )

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(log)

            # filter on data
            proxy_metric_prefetch = Prefetch(
                "proxy_metrics",
                queryset=models.ProxyMetrics.objects.filter(
                    region__region_code__startswith=query_country
                ).filter(region__resolution="NUTS0"),
            )

            queryset = (
                models.VarDetails.objects.filter(var_name=query_variable)
                .prefetch_related(proxy_metric_prefetch)
                .prefetch_related("taggings")
            )

            return queryset

        raise ValidationError(detail="Invalid API key")

    def get_serializer_class(self) -> ModelSerializer:
        """Get serialized data corresponding to the query."""
        serializer = serializers.SingleVarVarDetailsSerializer

        return serializer

    # swagger documentation
    api_key_param = openapi.Parameter(
        "api_key",
        in_=openapi.IN_QUERY,
        description="The super-secret API key",
        required=True,
        type=openapi.TYPE_STRING,
    )

    variable_param = openapi.Parameter(
        "variable",
        in_=openapi.IN_QUERY,
        description="Specify a desired variable",
        required=True,
        type=openapi.TYPE_STRING,
    )

    country_param = openapi.Parameter(
        "country",
        in_=openapi.IN_QUERY,
        description="Specify the country of the desired region",
        required=True,
        type=openapi.TYPE_STRING,
    )

    @swagger_auto_schema(
        manual_parameters=[api_key_param, variable_param, country_param]
    )
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """Add swagger documentation."""
        response = super().get(request, *args, **kwargs)
        return response


class TagsViewSet(generics.ListAPIView):
    """Class to design the API view of tags."""

    # only get method is allowed
    http_method_names = ["get"]

    serializer_class = serializers.TagsSerializer

    def get_queryset(self) -> QuerySet:
        """Prepare query."""
        # get API key
        query_api_key = self.request.query_params.get("api_key", None)

        # reveal data only if API_KEY matches
        if query_api_key == API_KEY:
            return models.Tags.objects.all()

        raise ValidationError(detail="Invalid API key")

    def list(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        """Process queried data."""
        response = super().list(request, args, kwargs)

        return response
