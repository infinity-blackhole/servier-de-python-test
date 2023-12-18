"""Input/output utilities for the Servier de Python project."""

import typing

import pandas as pd
import apache_beam as beam


class ClinicalTrial(typing.NamedTuple):
    """Represents a clinical trial."""

    id: str
    title: str
    date: pd.Timestamp
    journal: str


class Drug(typing.NamedTuple):
    """Represents a drug."""

    id: str
    name: str


class Pubmed(typing.NamedTuple):
    """Represents a publication in PubMed."""

    id: str
    title: str
    date: pd.Timestamp
    journal: str


class Mention(typing.NamedTuple):
    """Represents a mention of a drug in a publication."""

    drug_id: str
    drug_name: str
    publication_type: str
    publication_id: str
    publication_title: str
    publication_date: pd.Timestamp
    publication_journal: str


class ReadFromClinicalTrials(beam.PTransform):
    """A Beam PTransform for reading clinical trials from a CSV file."""

    def __init__(self, path: str):
        self.path = path

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection[ClinicalTrial]:
        return (
            pcoll
            | beam.io.ReadFromCsv(
                self.path,
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
            | beam.Map(
                lambda row: ClinicalTrial(
                    id=row.id,
                    title=row.title,
                    date=row.date,
                    journal=row.journal,
                )
            )
        )


class ReadFromDrugs(beam.PTransform):
    """A Beam PTransform for reading drugs from a CSV file."""

    def __init__(self, path: str):
        self.path = path

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection[Drug]:
        return (
            pcoll
            | beam.io.ReadFromCsv(
                self.path,
                header=0,
                names=["id", "name"],
                dtype={"id": str, "name": str},
                skip_blank_lines=True,
            )
            | beam.Map(
                lambda row: Drug(
                    id=row.id,
                    name=row.name,
                )
            )
        )


class ReadFromPubmedsCsv(beam.PTransform):
    """A Beam PTransform for reading publications from a CSV file in PubMed format."""

    def __init__(self, path: str):
        self.path = path

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection[Pubmed]:
        return (
            pcoll
            | beam.io.ReadFromCsv(
                self.path,
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
            | beam.Map(
                lambda row: Pubmed(
                    id=row.id,
                    title=row.title,
                    date=row.date,
                    journal=row.journal,
                )
            )
        )


class ReadFromPubmedsJson(beam.PTransform):
    """A Beam PTransform for reading publications from a JSON file in PubMed format."""

    def __init__(self, path: str):
        self.path = path

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection[Pubmed]:
        return (
            pcoll
            | beam.io.ReadFromJson(
                self.path,
                orient="records",
                lines=False,
                dtype={
                    "id": str,
                    "title": str,
                    "date": "datetime64[ns]",
                    "journal": str,
                },
            )
            | beam.Map(
                lambda row: Pubmed(
                    id=row.id,
                    title=row.title,
                    date=row.date,
                    journal=row.journal,
                )
            )
        )


class ReadFromPubmeds(beam.PTransform):
    """A Beam PTransform for reading publications from either a CSV or JSON file in PubMed format."""

    def __init__(self, path: str):
        self.path = path

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection[Pubmed]:
        if self.path.endswith(".csv"):
            return pcoll | ReadFromPubmedsCsv(self.path)
        elif self.path.endswith(".json"):
            return pcoll | ReadFromPubmedsJson(self.path)
        raise ValueError(f"Unknown file extension for {self.path}")


class WriteDrugMention(beam.PTransform):
    """A Beam PTransform for writing drug mentions to a JSON file."""

    def __init__(self, path: str):
        self.path = path

    def expand(self, pcoll: beam.PCollection[Mention]) -> beam.PCollection:
        return pcoll | beam.io.WriteToJson(
            self.path,
            orient="records",
            date_format="iso",
            lines=True,
        )
