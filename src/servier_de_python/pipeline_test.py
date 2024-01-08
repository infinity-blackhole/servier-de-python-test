import apache_beam as beam
import pandas as pd
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from servier_de_python.io import (
    ClinicalTrial,
    Drug,
    Mention,
    Pubmed,
    ReadFromClinicalTrials,
    ReadFromDrugs,
    ReadFromPubmeds,
)
from servier_de_python.testing.fixtures import (
    simbple_drugs_path,
    simple_clinical_trials_path,
    simple_pubmeds_path,
)
from servier_de_python.transforms import (
    MatchDrugMentions,
    SplitClinicalTrialTitleByWord,
    SplitPubmedTitleByWord,
)


def test_clinical_trial_title_by_word():
    with TestPipeline() as p:
        out = (
            p
            | ReadFromClinicalTrials(
                simple_clinical_trials_path,
            )
            | SplitClinicalTrialTitleByWord()
        )

        assert_that(
            out,
            equal_to(
                [
                    (
                        "FOO",
                        ClinicalTrial(
                            id="123",
                            title="Foo Bar",
                            date=pd.Timestamp("2020-01-01 00:00:00"),
                            journal="Journal of emergency nursing",
                        ),
                    ),
                    (
                        "BAR",
                        ClinicalTrial(
                            id="123",
                            title="Foo Bar",
                            date=pd.Timestamp("2020-01-01 00:00:00"),
                            journal="Journal of emergency nursing",
                        ),
                    ),
                ],
            ),
        )


def test_pubmed_title_by_word():
    with TestPipeline() as p:
        out = p | ReadFromPubmeds(simple_pubmeds_path) | SplitPubmedTitleByWord()

        assert_that(
            out,
            equal_to(
                [
                    (
                        "FOO",
                        Pubmed(
                            id="1",
                            title="Foo Bar",
                            date=pd.Timestamp("2019-01-01 00:00:00"),
                            journal="Journal of emergency nursing",
                        ),
                    ),
                    (
                        "BAR",
                        Pubmed(
                            id="1",
                            title="Foo Bar",
                            date=pd.Timestamp("2019-01-01 00:00:00"),
                            journal="Journal of emergency nursing",
                        ),
                    ),
                ]
            ),
        )


def test_drugs():
    with TestPipeline() as p:
        out = p | ReadFromDrugs(simbple_drugs_path)

        assert_that(
            out,
            equal_to(
                [
                    Drug(id="ABC", name="FOO"),
                ]
            ),
        )


def test_match_drug_mentions():
    with TestPipeline() as p:
        drugs = p | "CreateDrug" >> beam.Create(
            [
                Drug(id="ABC", name="FOO"),
            ]
        )
        pubmeds = p | "CreatePubMed" >> beam.Create(
            [
                (
                    "FOO",
                    Pubmed(
                        id="1",
                        title="Foo Bar",
                        date=pd.Timestamp("2019-01-01 00:00:00"),
                        journal="Journal of emergency nursing",
                    ),
                ),
                (
                    "BAR",
                    Pubmed(
                        id="1",
                        title="Foo Bar",
                        date=pd.Timestamp("2019-01-01 00:00:00"),
                        journal="Journal of emergency nursing",
                    ),
                ),
            ]
        )
        clinical_trials = p | "CreateClinicalTrials" >> beam.Create(
            [
                (
                    "FOO",
                    ClinicalTrial(
                        id="123",
                        title="Foo Bar",
                        date=pd.Timestamp("2020-01-01 00:00:00"),
                        journal="Journal of emergency nursing",
                    ),
                ),
                (
                    "BAR",
                    ClinicalTrial(
                        id="123",
                        title="Foo Bar",
                        date=pd.Timestamp("2020-01-01 00:00:00"),
                        journal="Journal of emergency nursing",
                    ),
                ),
            ]
        )

        out = drugs | MatchDrugMentions(pubmed=pubmeds, clinical_trials=clinical_trials)

        assert_that(
            out,
            equal_to(
                [
                    Mention(
                        drug_id="ABC",
                        drug_name="FOO",
                        publication_type="PUBMED",
                        publication_id="1",
                        publication_title="Foo Bar",
                        publication_date=pd.Timestamp("2019-01-01 00:00:00"),
                        publication_journal="Journal of emergency nursing",
                    ),
                    Mention(
                        drug_id="ABC",
                        drug_name="FOO",
                        publication_type="CLINICAL_TRIAL",
                        publication_id="123",
                        publication_title="Foo Bar",
                        publication_date=pd.Timestamp("2020-01-01 00:00:00"),
                        publication_journal="Journal of emergency nursing",
                    ),
                ]
            ),
        )
