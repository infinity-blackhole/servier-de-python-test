"""A module that contains Beam transforms for the Servier DE Python test."""

import string
import typing

import apache_beam as beam
from apache_beam.transforms.ptransform import ptransform_fn

from servier_de_python.schemas import Drug

from .io import ClinicalTrial, Mention, Pubmed

T = typing.TypeVar("T")


@ptransform_fn
def SplitTitleByWord(
    pcoll: beam.PCollection[T],
) -> beam.PCollection[typing.Tuple[str, T]]:
    """A Beam transform that splits the title of an element by word."""

    def _split(element):
        words = element.title.translate(
            str.maketrans("", "", string.punctuation)
        ).split()
        for word in words:
            yield (word.upper(), element)

    return pcoll | beam.FlatMap(_split)


@ptransform_fn
def SplitClinicalTrialTitleByWord(
    pcoll: beam.PCollection[ClinicalTrial],
) -> beam.PCollection[typing.Tuple[str, ClinicalTrial]]:
    """A Beam transform that splits the title of a ClinicalTrial element by word."""

    return pcoll | "ClinicalTrialTitleByWord" >> SplitTitleByWord()


@ptransform_fn
def SplitPubmedTitleByWord(
    pcoll: beam.PCollection[Pubmed],
) -> beam.PCollection[typing.Tuple[str, Pubmed]]:
    """A Beam transform that splits the title of a Pubmed element by word."""
    return pcoll | "PubmedTitleByWord" >> SplitTitleByWord()


@ptransform_fn
def MatchClinicalTrialDrugs(
    pcoll: beam.PCollection[Drug],
    clinical_trials: beam.PCollection[ClinicalTrial],
) -> beam.PCollection[Mention]:
    """A Beam transform that matches drugs mentioned in ClinicalTrial elements."""

    def _unnest(element):
        for drug in element[1]["drugs"]:
            for clinical_trial in element[1]["clinical_trials"]:
                yield Mention(
                    drug_id=drug.id,
                    drug_name=drug.name,
                    publication_type="CLINICAL_TRIAL",
                    publication_id=clinical_trial.id,
                    publication_title=clinical_trial.title,
                    publication_date=clinical_trial.date,
                    publication_journal=clinical_trial.journal,
                )

    return (
        {
            "drugs": pcoll,
            "clinical_trials": clinical_trials,
        }
        | beam.CoGroupByKey()
        | beam.FlatMap(_unnest)
    )


@ptransform_fn
def MatchPubmedDrugs(
    pcoll: beam.PCollection[Drug],
    pubmed: beam.PCollection[Pubmed],
) -> beam.PCollection[Mention]:
    """A Beam transform that matches drugs mentioned in Pubmed elements."""

    def _unnest(element):
        for drug in element[1]["drugs"]:
            for pubmed in element[1]["pubmed"]:
                yield Mention(
                    drug_id=drug.id,
                    drug_name=drug.name,
                    publication_type="PUBMED",
                    publication_id=pubmed.id,
                    publication_title=pubmed.title,
                    publication_date=pubmed.date,
                    publication_journal=pubmed.journal,
                )

    return (
        {"drugs": pcoll, "pubmed": pubmed} | beam.CoGroupByKey() | beam.FlatMap(_unnest)
    )


@ptransform_fn
def MatchDrugs(
    pcoll: beam.PCollection[Drug],
    clinical_trials: beam.PCollection[ClinicalTrial],
    pubmed: beam.PCollection[Pubmed],
) -> beam.PCollection[Mention]:
    """A Beam transform that matches drugs mentioned in Pubmed elements."""

    clinical_trials_mentions = pcoll | MatchClinicalTrialDrugs(clinical_trials)
    pubmed_mentions = pcoll | MatchPubmedDrugs(pubmed)

    return (
        clinical_trials_mentions,
        pubmed_mentions,
    ) | beam.Flatten().with_output_types(Mention)
