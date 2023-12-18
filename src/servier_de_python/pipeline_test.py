import pandas as pd
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from servier_de_python.io import (
    ClinicalTrial,
    Drug,
    Pubmed,
    ReadFromClinicalTrials,
    ReadFromDrugs,
    ReadFromPubmeds,
)
from servier_de_python.testing.fixtures import (
    simple_clinical_trials_path,
    simple_pubmeds_path,
    simbple_drugs_path,
)
from servier_de_python.transforms import (
    SplitClinicalTrialTitleByWord,
    SplitPubmedTitleByWord,
)


def test_clinical_trial_title_by_word():
    with TestPipeline() as p:
        out = (
            p
            | "ReadFiles"
            >> ReadFromClinicalTrials(
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
        out = (
            p
            | "ReadFiles" >> ReadFromPubmeds(simple_pubmeds_path)
            | SplitPubmedTitleByWord()
        )

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
        out = p | "ReadFiles" >> ReadFromDrugs(simbple_drugs_path)

        assert_that(
            out,
            equal_to(
                [
                    Drug(id="ABC", name="FOO"),
                ]
            ),
        )
