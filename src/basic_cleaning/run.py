#!/usr/bin/env python
"""
Download from W&B  the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
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
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path, low_memory=False)

    # Dropping outliers based on min and max
    logger.info("Dropping outliers and null for date/reviews")

    min_val = args.min_price
    max_val = args.max_price
    # subset based on min and max_val
    # Just picked 2018-01-01 as the date for filling the NAs
    idx = df['price'].between(min_val, max_val)
    df = df[idx].copy()

    logger.info("Adding this for new data sample handling")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    
    artifact.add_file("clean_sample.csv")

    logger.info("Logging artifact")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Name for the input artifact",## INSERT DESCRIPTION HERE,
        required=True,
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Name for the output artifact",## INSERT DESCRIPTION HERE,
        required=True,
    )

    parser.add_argument(
        "--output_type", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Output artifact type",## INSERT DESCRIPTION HERE,
        required=True,
    )

    parser.add_argument(
        "--output_description", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Description for the artifact",## INSERT DESCRIPTION HERE,
        required=True,
    )

    parser.add_argument(
        "--min_price", 
        type=float,## INSERT TYPE HERE: str, float or int,
        help="Min price",## INSERT DESCRIPTION HERE,
        required=True,
    )

    parser.add_argument(
        "--max_price", 
        type=float,## INSERT TYPE HERE: str, float or int,
        help="Max price",## INSERT DESCRIPTION HERE,
        required=True,
    )


    args = parser.parse_args()

    go(args)
