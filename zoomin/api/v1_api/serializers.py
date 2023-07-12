"""Django serializer module."""
import typing
from rest_framework.serializers import ModelSerializer

from zoomin.api.v1_api import models


class FlattenMixin(object):
    """Class to flaten a given serializer."""

    @typing.no_type_check
    def to_representation(self, obj):
        """Flaten a given serializer."""
        serializer_class = self.__class__.__name__
        if not hasattr(self.Meta, "flatten"):
            AttributeError(f"Class {serializer_class} missing 'Meta.flatten' attribute")
        # Get the current object representation
        rep = super().to_representation(obj)
        # Iterate the specified related objects with their serializer
        for field, serializer_class in self.Meta.flatten:
            serializer = serializer_class(context=self.context)
            objrep = serializer.to_representation(getattr(obj, field))
            for key in objrep:
                # INFO: To Include their fields, prefixed, in the current representation, use the below line
                # rep[field + "__" + key] = objrep[key]
                rep[key] = objrep[key]
        return rep


# =============================================================================================================
# Single Region - All Data
# =============================================================================================================
# citations
class CitationsSerializer(ModelSerializer):
    """Citations table serializer."""

    class Meta:
        """Meta class."""

        model = models.Citations
        exclude = ["id"]


# tags
class TagsSerializer(ModelSerializer):
    """Tags table serializer."""

    class Meta:
        """Meta class."""

        model = models.Tags
        exclude = [
            "id",
        ]


class TaggingsSerializer(FlattenMixin, ModelSerializer):
    """Taggings table serializer."""

    class Meta:
        """Meta class."""

        model = models.Taggings
        exclude = ["id", "tag", "taggable_var"]
        flatten = [
            ("tag", TagsSerializer),
        ]


# proxy metric mapping
class ProxyMetricVarDetailsSerializer(ModelSerializer):
    """Var details table serializer."""

    class Meta:
        """Meta class."""

        model = models.VarDetails
        fields = ["var_name"]


class ProxyMetricsSerializer(FlattenMixin, ModelSerializer):
    """Proxy metrics serializer."""

    class Meta:
        """Meta class."""

        model = models.ProxyMetrics
        exclude = [
            "id",
            "region",
            "var_detail",
            "proxy_var_detail",
        ]
        flatten = [
            ("proxy_var_detail", ProxyMetricVarDetailsSerializer),
        ]


# var meta data
class VarDetailsSerializer(ModelSerializer):
    """Var details table serializer."""

    taggings = TaggingsSerializer(many=True)
    proxy_metrics = ProxyMetricsSerializer(many=True)

    class Meta:
        """Meta class."""

        model = models.VarDetails
        fields = [
            "var_name",
            "var_description",
            "var_unit",
            "var_aggregation_method",
            "taggings",
            "proxy_metrics",
        ]


class PathwaysSerializer(ModelSerializer):
    """Pathways serializer."""

    class Meta:
        """Meta class."""

        model = models.Pathways
        fields = ["pathway_main", "pathway_reference", "pathway_variant"]


class OriginalResolutionsSerializer(ModelSerializer):
    """Original resolutions table serializer."""

    class Meta:
        """Meta class."""

        model = models.OriginalResolutions
        exclude = ["id"]


class ClimateExperimentsSerializer(ModelSerializer):
    """Climate experiments table serializer."""

    class Meta:
        """Meta class."""

        model = models.ClimateExperiments
        exclude = ["id"]


class DisaggregationMethodsSerializer(ModelSerializer):
    """Disaggregation methods table serializer."""

    class Meta:
        """Meta class."""

        model = models.DisaggregationMethods
        exclude = ["id"]


# data
class SingleRegionAllDataSerializer(FlattenMixin, ModelSerializer):
    """Region data serializer."""

    class Meta:
        """Meta class."""

        model = models.RegionData
        fields = [
            "value",
            "year",
            "confidence_interval",
            "quality_rating",
        ]

        flatten = [
            ("var_detail", VarDetailsSerializer),
            ("pathway", PathwaysSerializer),
            ("citation", CitationsSerializer),
            ("original_resolution", OriginalResolutionsSerializer),
            ("climate_experiment", ClimateExperimentsSerializer),
            ("disaggregation_method", DisaggregationMethodsSerializer),
        ]


# regions
class RegionsSerializer(FlattenMixin, ModelSerializer):
    """Regions serializer."""

    class Meta:
        """Meta class."""

        model = models.Regions
        fields = [
            "resolution",
            "region_code",
            "region_name",
            "parent_region_code",
            "year",
        ]
        flatten = [
            ("citation", CitationsSerializer),
        ]


class SingleRegionAllDataSerializer(FlattenMixin, ModelSerializer):
    """The serializer is used to query a single region data for all variables."""

    class Meta:
        """Meta class."""

        model = models.RegionData
        fields = [
            "value",
            "year",
            "confidence_interval",
            "quality_rating",
        ]

        flatten = [
            ("var_detail", VarDetailsSerializer),
            ("pathway", PathwaysSerializer),
            ("citation", CitationsSerializer),
            ("original_resolution", OriginalResolutionsSerializer),
            ("climate_experiment", ClimateExperimentsSerializer),
            ("disaggregation_method", DisaggregationMethodsSerializer),
        ]


# =============================================================================================================
# Single Region - All Data - Mini Version (Fewer fields)
# =============================================================================================================
class VarDetailsMiniVersionSerializer(ModelSerializer):
    """Var details table serializer."""

    taggings = TaggingsSerializer(many=True)

    class Meta:
        """Meta class."""

        model = models.VarDetails
        fields = [
            "var_name",
            "var_unit",
            "taggings",
        ]


class PathwaysMiniVersionSerializer(ModelSerializer):
    """Pathways serializer."""

    class Meta:
        """Meta class."""

        model = models.Pathways
        fields = ["pathway_main", "pathway_variant"]


class SingleRegionAllDataMiniVersionSerializer(FlattenMixin, ModelSerializer):
    """Region data serializer."""

    class Meta:
        """Meta class."""

        model = models.RegionData
        fields = [
            "value",
            "year",
        ]

        flatten = [
            ("var_detail", VarDetailsMiniVersionSerializer),
            ("pathway", PathwaysMiniVersionSerializer),
            ("climate_experiment", ClimateExperimentsSerializer),
        ]


# =============================================================================================================
# Single Variable - All Regions
# =============================================================================================================
class SingleVarRegionsSerializer(ModelSerializer):
    """The serializer is used to query a single var data for all LAU regions of a country."""

    class Meta:
        """Meta class."""

        model = models.Regions
        fields = [
            "region_code",
            "parent_region_code",
        ]


class SingleVarVarDetailsSerializer(ModelSerializer):
    """The serializer is used to query a single var data for all LAU regions of a country."""

    proxy_metrics = ProxyMetricsSerializer(many=True)
    taggings = TaggingsSerializer(many=True)

    class Meta:
        """Meta class."""

        model = models.VarDetails
        fields = [
            "var_name",
            "var_description",
            "var_unit",
            "var_aggregation_method",
            "proxy_metrics",
            "taggings",
        ]


class SingleVarRegionDataSerializer(FlattenMixin, ModelSerializer):
    """The serializer is used to query a single var data for all LAU regions of a country."""

    class Meta:
        """Meta class."""

        model = models.RegionData
        fields = [
            "value",
            "year",
            "confidence_interval",
            "quality_rating",
        ]

        flatten = [
            ("region", SingleVarRegionsSerializer),
            ("pathway", PathwaysSerializer),
            ("citation", CitationsSerializer),
            ("original_resolution", OriginalResolutionsSerializer),
            ("climate_experiment", ClimateExperimentsSerializer),
            ("disaggregation_method", DisaggregationMethodsSerializer),
        ]
