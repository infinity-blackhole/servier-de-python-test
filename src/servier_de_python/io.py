"""Input/output utilities for the Servier de Python project."""

import apache_beam as beam
from apache_beam.transforms.ptransform import ptransform_fn

from servier_de_python.schemas import (
    ClinicalTrial,
    Drug,
    Mention,
    Pubmed,
    RowAsClinicalTrial,
    RowAsDrug,
    RowAsPubmed,
)


@ptransform_fn
def ReadFromClinicalTrials(
    pcoll: beam.PCollection, path: str
) -> beam.PCollection[ClinicalTrial]:
    """A Beam PTransform for reading clinical trials from a CSV file."""

    return (
        pcoll
        | beam.io.ReadFromCsv(
            path,
            header=0,
            names=["id", "title", "date", "journal"],
            dtype={
                "id": str,
                "title": str,
                "date": str,
                "journal": str,
            },
            skip_blank_lines=True,
            parse_dates=["date"],
            infer_datetime_format=True,
            dayfirst=True,
            cache_dates=True,
        )
        | RowAsClinicalTrial()
    )


@ptransform_fn
def ReadFromDrugs(pcoll: beam.PCollection, path: str) -> beam.PCollection[Drug]:
    """A Beam PTransform for reading drugs from a CSV file."""

    return (
        pcoll
        | beam.io.ReadFromCsv(
            path,
            header=0,
            names=["id", "name"],
            dtype={"id": str, "name": str},
            skip_blank_lines=True,
        )
        | RowAsDrug()
    )


@ptransform_fn
def ReadFromPubmedsCsv(pcoll: beam.PCollection, path: str) -> beam.PCollection[Pubmed]:
    """A Beam PTransform for reading publications from a CSV file in PubMed format."""

    return (
        pcoll
        | beam.io.ReadFromCsv(
            path,
            header=0,
            names=["id", "title", "date", "journal"],
            dtype={
                "id": str,
                "title": str,
                "date": str,
                "journal": str,
            },
            skip_blank_lines=True,
            parse_dates=["date"],
            infer_datetime_format=True,
            dayfirst=True,
            cache_dates=True,
        )
        | RowAsPubmed()
    )


@ptransform_fn
def ReadFromPubmedsJson(pcoll: beam.PCollection, path: str) -> beam.PCollection[Pubmed]:
    """A Beam PTransform for reading publications from a JSON file in PubMed format."""

    return (
        pcoll
        | beam.io.ReadFromJson(
            path,
            orient="records",
            lines=False,
            dtype={
                "id": str,
                "title": str,
                "date": "datetime64[ns]",
                "journal": str,
            },
        )
        | RowAsPubmed()
    )


@ptransform_fn
def ReadFromPubmeds(pcoll: beam.PCollection, path: str) -> beam.PCollection[Pubmed]:
    """A Beam PTransform for reading publications from either a CSV or JSON file in PubMed format."""

    if path.endswith(".csv"):
        return pcoll | ReadFromPubmedsCsv(path)
    elif path.endswith(".json"):
        return pcoll | ReadFromPubmedsJson(path)
    raise ValueError(f"Unknown file extension for {path}")


@ptransform_fn
def WriteDrugMentions(
    pcoll: beam.PCollection[Mention], path: str
) -> beam.PCollection[Mention]:
    """A Beam PTransform for writing drug mentions to a JSON file."""

    return pcoll | beam.io.WriteToJson(
        path,
        orient="records",
        date_format="iso",
        lines=True,
    )
