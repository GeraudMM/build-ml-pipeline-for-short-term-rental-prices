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

    logger.info("Download the artifact to clean")
    artifact = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact)

    logger.info("Clean the artifact")
    idx = df['price'].between(args.min_price, args.max_price) # Remove outlier prices
    df = df[idx].copy()
    
    df['last_review'] = pd.to_datetime(df['last_review']) # Convert last_review to datetime

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    logger.info("Save the cleaned the artifact")
    df.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    logger.info(f"Succesfully run the basic_cleaning step !")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="name of the input artifact containing a csv file to clean",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="output artifact containing the cleaned csv file",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Describe the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="min price after cleaning the dataframe",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="max price after cleaning the dataframe",
        required=True
    )


    args = parser.parse_args()

    go(args)
