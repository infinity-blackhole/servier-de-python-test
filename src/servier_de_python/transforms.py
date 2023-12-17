"""A module that contains Beam transforms for the Servier DE Python test."""

import string
import typing

import apache_beam as beam

from .io import ClinicalTrial, Mention, Pubmed


class SplitTitleByWord(beam.PTransform):
    """A Beam transform that splits the title of an element by word."""

    def expand(self, pcoll):
        def _split(element):
            words = element.title.translate(
                str.maketrans("", "", string.punctuation)
            ).split()
            for word in words:
                yield (word.upper(), element)

        return pcoll | beam.FlatMap(_split).with_output_types(
            typing.Tuple[str, typing.Any]
        )


class SplitClinicalTrialTitleByWord(beam.PTransform):
    """A Beam transform that splits the title of a ClinicalTrial element by word."""

    def expand(self, pcoll):
        return (
            pcoll
            | "ClinicalTrialTitleByWord"
            >> SplitTitleByWord().with_output_types(typing.Tuple[str, ClinicalTrial])
        )


class SplitPubmedTitleByWord(beam.PTransform):
    """A Beam transform that splits the title of a Pubmed element by word."""

    def expand(self, pcoll):
        return pcoll | "PubmedTitleByWord" >> SplitTitleByWord().with_output_types(
            typing.Tuple[str, Pubmed]
        )


class MatchClinicalTrialDrugs(beam.PTransform):
    """A Beam transform that matches drugs mentioned in ClinicalTrial elements."""

    def expand(
        self, pcolls: typing.Tuple[beam.PCollection, beam.PCollection]
    ) -> beam.PCollection:
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
                "drugs": pcolls[0],
                "clinical_trials": pcolls[1],
            }
            | beam.CoGroupByKey()
            | beam.FlatMap(_unnest).with_output_types(Mention)
        )


class MatchPubmedDrugs(beam.PTransform):
    """A Beam transform that matches drugs mentioned in Pubmed elements."""

    def expand(
        self, pcolls: typing.Tuple[beam.PCollection, beam.PCollection]
    ) -> beam.PCollection:
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
            {
                "drugs": pcolls[0],
                "pubmed": pcolls[1],
            }
            | beam.CoGroupByKey()
            | beam.FlatMap(_unnest).with_output_types(Mention)
        )
