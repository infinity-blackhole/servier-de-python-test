"""Input/output utilities for the Servier de Python project."""

import typing

import apache_beam as beam
import pandas as pd
from apache_beam.transforms.ptransform import ptransform_fn


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


@ptransform_fn
def RowAsClinicalTrial(pcoll: beam.PCollection) -> beam.PCollection[ClinicalTrial]:
    return pcoll | beam.Map(
        lambda row: ClinicalTrial(
            id=row.id,
            title=row.title,
            date=row.date,
            journal=row.journal,
        )
    )


@ptransform_fn
def RowAsDrug(pcoll: beam.PCollection) -> beam.PCollection[Drug]:
    return pcoll | beam.Map(
        lambda row: Drug(
            id=row.id,
            name=row.name,
        )
    )


@ptransform_fn
def RowAsPubmed(pcoll: beam.PCollection) -> beam.PCollection[Pubmed]:
    return pcoll | beam.Map(
        lambda row: Pubmed(
            id=row.id,
            title=row.title,
            date=row.date,
            journal=row.journal,
        )
    )
