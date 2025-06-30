# ACLIS

Agentic Customer Lifecycle Intelligence Suite

## Overview

ACLIS is an agentic AI system for customer lifecycle intelligence. It integrates machine learning outputs, feature metadata, and offer data to:

- Dynamically segment customers based on predictive analytics and feature insights.
- Identify gaps in current offerings and recommend packages tailored to each segment.

## Data Architecture

All raw data is stored in the `data/` directory:

- `data/scored_data.csv`: Output of a trained ML model with key score columns (`speed_upsell`, `y_pred`, `decile`, `percentile`, `y_pred_calibrated`) and input features.
- `data/column_and_feature_metadata.csv`: Descriptions and metadata for each feature/column in `scored_data.csv`.
- `data/offers/`: Individual CSV files for each available package or offer.
- `data/offers/offers_metadata.csv`: Descriptions of columns in the offer files.

## Core Pipelines

1. **Ingestion Pipeline**
   - Loads all CSVs from `data/` into a unified, searchable store.
   - Uses indexing (e.g., via llamaindex or LangChain) for fast retrieval.

2. **Enrichment Pipeline**
   - Generates contextual user profiles by combining customer metrics with feature metadata.
   - Leverages the Gemma LLM (via LangChain-Ollama) to produce rich, narrative descriptions of each user or batch of users.
   - Batches inputs to maximize model context window and throughput.

3. **Segmentation & Insights**
   - Automatically defines customer segments based on enriched profiles and predictive scores.
   - Produces insights at both the overall population and individual segment levels.

4. **Offer Gap Analysis**
   - Compares current customer segments against available offers.
   - Identifies missing or underutilized packages and suggests new or adjusted offers.

## Next Steps

- Implement the ingestion pipeline to centralize data loading and indexing.
- Build the enrichment module to generate user profiles at scale.
- Develop segmentation logic and integrate with insights reporting.
- Create offer gap analysis routines to drive recommendation outputs.

## Technologies

- Python 3.x
- llamaindex, LangChain (with Ollama Gemma model)
- pandas, numpy for data processing
