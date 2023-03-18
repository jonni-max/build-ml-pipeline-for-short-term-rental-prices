#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading and reading input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)
    
    logger.info("Filtering offers with respect to price")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    
    logger.info("Saving clean artifact and uploading")
    df.to_csv("clean_sample.csv", index=False)
    
    artifact = wandb.Artifact(
         args.output_artifact,
         type=args.output_type,
         description=args.output_description
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data clening")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="The name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="A description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The lowest price of an offer accepted by the statkeholders",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The highest price of an offer accepted by the stakeholders",
        required=True
    )


    args = parser.parse_args()

    go(args)
