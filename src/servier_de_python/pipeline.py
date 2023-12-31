"""Pipeline to find drug mentions in clinical trials and pubmed articles. """

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from servier_de_python.io import (
    ReadFromClinicalTrials,
    ReadFromDrugs,
    ReadFromPubmeds,
    WriteDrugMentions,
)
from servier_de_python.transforms import (
    MatchDrugMentions,
    SplitClinicalTrialTitleByWord,
    SplitPubmedTitleByWord,
)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clinical-trials-dataset-path",
        required=True,
        help="Path to the clinical trials dataset.",
        nargs="+",
    )
    parser.add_argument(
        "--drugs-dataset-path",
        required=True,
        help="Path to the drugs dataset.",
        nargs="+",
    )
    parser.add_argument(
        "--pubmed-dataset-path",
        required=True,
        help="Path to the pubmed dataset.",
        nargs="+",
    )
    parser.add_argument(
        "--output", required=True, help="Output file to write results to."
    )
    args, options = parser.parse_known_args()

    with beam.Pipeline(options=PipelineOptions(options)) as p:
        clinical_trials = (
            [
                p | f"ReadFromClinicalTrials{i}" >> ReadFromClinicalTrials(path)
                for i, path in enumerate(args.clinical_trials_dataset_path)
            ]
            | "FlattenClinicalTrial" >> beam.Flatten()
            | SplitClinicalTrialTitleByWord()
        )
        pubmed = (
            [
                p | f"ReadFromPubmeds{i}" >> ReadFromPubmeds(path)
                for i, path in enumerate(args.pubmed_dataset_path)
            ]
            | "FlattenPubmed" >> beam.Flatten()
            | SplitPubmedTitleByWord()
        )
        drugs = [
            p | f"ReadFromDrugs{i}" >> ReadFromDrugs(path)
            for i, path in enumerate(args.drugs_dataset_path)
        ] | "FlattenDrug" >> beam.Flatten()

        (
            drugs
            | MatchDrugMentions(clinical_trials=clinical_trials, pubmed=pubmed)
            | WriteDrugMentions(args.output)
        )


if __name__ == "__main__":
    run()
