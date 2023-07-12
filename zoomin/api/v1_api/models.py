"""Django model module.

* Make sure each model has one field with primary_key=True
* Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
* Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
"""
from django.db import models

# citations
class Citations(models.Model):
    """Citations table."""

    id = models.AutoField(primary_key=True)
    data_source_name = models.CharField(max_length=255)
    data_source_link = models.URLField()
    data_source_citation = models.TextField(max_length=255)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "citations"


# regions
class Regions(models.Model):
    """Regions table."""

    id = models.AutoField(primary_key=True)
    citation = models.ForeignKey(Citations, models.DO_NOTHING)
    resolution = models.CharField(max_length=255)
    year = models.IntegerField()
    region_code = models.CharField(max_length=255)
    region_name = models.CharField(max_length=255)
    parent_region_code = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id", "region_code"])]
        db_table = "regions"


# var details
class VarDetails(models.Model):
    """Var details table."""

    id = models.AutoField(primary_key=True)
    var_name = models.TextField(max_length=255)
    var_description = models.TextField(max_length=255, blank=True, null=True)
    var_unit = models.CharField(max_length=255)
    var_aggregation_method = models.CharField(max_length=255)
    on_the_fly_calculation = models.TextField(max_length=255, blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "var_details"


# EUCalc pathways
class Pathways(models.Model):
    """Pathways table."""

    id = models.AutoField(primary_key=True)
    pathway_main = models.CharField(max_length=255, blank=True, null=True)
    pathway_reference = models.URLField(blank=True, null=True)
    pathway_variant = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "pathways"


# original resolutions
class OriginalResolutions(models.Model):
    """Original resolutions table."""

    id = models.AutoField(primary_key=True)
    original_resolution = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "original_resolutions"


# climate experiments
class ClimateExperiments(models.Model):
    """Climate experiments table."""

    id = models.AutoField(primary_key=True)
    climate_experiment = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "climate_experiments"


# disaggregation methods
class DisaggregationMethods(models.Model):
    """Disaggregation methods table."""

    id = models.AutoField(primary_key=True)
    disaggregation_method = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "disaggregation_methods"


# data
class RegionData(models.Model):
    """Var values table."""

    id = models.AutoField(primary_key=True)
    region = models.ForeignKey(
        Regions,
        models.DO_NOTHING,
        related_name="region_data",
    )
    var_detail = models.ForeignKey(
        VarDetails, models.DO_NOTHING, related_name="region_data"
    )
    pathway = models.ForeignKey(Pathways, models.DO_NOTHING, blank=True, null=True)
    original_resolution = models.ForeignKey(OriginalResolutions, models.DO_NOTHING)
    climate_experiment = models.ForeignKey(
        ClimateExperiments, models.DO_NOTHING, blank=True, null=True
    )
    disaggregation_method = models.ForeignKey(
        DisaggregationMethods, models.DO_NOTHING, blank=True, null=True
    )
    citation = models.ForeignKey(Citations, models.DO_NOTHING)
    year = models.IntegerField(blank=True, null=True)
    value = models.FloatField()
    confidence_interval = models.FloatField(blank=True, null=True)
    quality_rating = models.CharField(max_length=255, blank=True, null=True)
    chosen = models.IntegerField(blank=True, null=True)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [
        #     models.Index(
        #         fields=[
        #             "id",
        #             "region",
        #             "var_detail",
        #             "pathway_id",
        #             "year",
        #         ]
        #     )
        # ]
        db_table = "region_data"


# proxy
class ProxyMetrics(models.Model):
    """Tags table."""

    id = models.AutoField(primary_key=True)
    region = models.ForeignKey(Regions, models.DO_NOTHING)
    var_detail = models.ForeignKey(
        VarDetails, models.DO_NOTHING, related_name="proxy_metrics"
    )
    proxy_var_detail = models.ForeignKey(
        VarDetails, models.DO_NOTHING, related_name="proxy_var_detail"
    )

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "proxy_metrics"


# tags
class Tags(models.Model):
    """Tags table."""

    id = models.AutoField(primary_key=True)
    tag_dimension = models.CharField(max_length=255)
    tag_name = models.CharField(max_length=255)

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "tags"


class Taggings(models.Model):
    """Taggings table."""

    id = models.AutoField(primary_key=True)
    tag = models.ForeignKey(Tags, models.DO_NOTHING, blank=True, null=True)
    taggable_var = models.ForeignKey(
        VarDetails,
        models.DO_NOTHING,
        blank=True,
        null=True,
        related_name="taggings",
    )

    class Meta:
        """Meta class."""

        managed = True
        # indexes = [models.Index(fields=["id"])]
        db_table = "taggings"
