"""A module that contains Beam transforms for the Servier DE Python test."""

import string
import typing
from difflib import SequenceMatcher

import apache_beam as beam
from apache_beam.transforms.ptransform import ptransform_fn

from servier_de_python.io import ClinicalTrial, Mention, Pubmed
from servier_de_python.schemas import Drug

T = typing.TypeVar("T")


@ptransform_fn
def SplitTitleByWord(
    pcoll: beam.PCollection[T],
) -> beam.PCollection[typing.Tuple[str, T]]:
    """A Beam transform that splits the title of an element by word."""

    def _split(element):
        for word in element.title.split():
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
def MatchClinicalTrialDrugMentions(
    pcoll: beam.PCollection[ClinicalTrial],
    drug: beam.PCollection[typing.Tuple[str, Drug]],
    threshold: float = 0.8,
) -> beam.PCollection[Mention]:
    """A Beam transform that matches drugs mentioned in ClinicalTrial elements."""

    def _join(element, drugs):
        for drug in drugs:
            matcher = SequenceMatcher(
                lambda x: x in string.punctuation,
                drug.name,
                element[0],
            )
            if matcher.ratio() < threshold:
                continue

            yield Mention(
                drug_id=drug.id,
                drug_name=drug.name,
                publication_type="CLINICAL_TRIAL",
                publication_id=element[1].id,
                publication_title=element[1].title,
                publication_date=element[1].date,
                publication_journal=element[1].journal,
            )

    return pcoll | beam.FlatMap(_join, drugs=beam.pvalue.AsList(drug))


@ptransform_fn
def MatchPubmedDrugMentions(
    pcoll: beam.PCollection[Pubmed],
    drug: beam.PCollection[typing.Tuple[str, Drug]],
    threshold: float = 0.8,
) -> beam.PCollection[Mention]:
    """A Beam transform that matches drugs mentioned in Pubmed elements."""

    def _join(element, drugs):
        for drug in drugs:
            matcher = SequenceMatcher(
                lambda x: x in string.punctuation,
                drug.name,
                element[0],
            )

            if matcher.ratio() < threshold:
                continue

            yield Mention(
                drug_id=drug.id,
                drug_name=drug.name,
                publication_type="PUBMED",
                publication_id=element[1].id,
                publication_title=element[1].title,
                publication_date=element[1].date,
                publication_journal=element[1].journal,
            )

    return pcoll | beam.FlatMap(_join, drugs=beam.pvalue.AsList(drug))


@ptransform_fn
def MatchDrugMentions(
    pcoll: beam.PCollection[Drug],
    clinical_trials: beam.PCollection[typing.Tuple[str, ClinicalTrial]],
    pubmeds: beam.PCollection[typing.Tuple[str, Pubmed]],
    threshold: float = 0.8,
) -> beam.PCollection[Mention]:
    """A Beam transform that matches drugs mentioned in Pubmed elements."""

    clinical_trials_mentions = clinical_trials | MatchClinicalTrialDrugMentions(
        drug=pcoll, threshold=threshold
    )
    pubmed_mentions = pubmeds | MatchPubmedDrugMentions(drug=pcoll, threshold=threshold)

    return (
        clinical_trials_mentions,
        pubmed_mentions,
    ) | beam.Flatten().with_output_types(Mention)
