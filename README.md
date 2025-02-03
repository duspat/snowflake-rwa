# Snowflake Real World Analytics - Live Session

Welcome to the repository for the online training session **"Snowflake Real World Analytics"**. This repo contains all the code, scripts, and resources used during the live session. Follow along with the training, run the examples, and leverage the code provided to deepen your understanding of Snowflake and real-world analytics.

## Overview

This training session is designed to provide practical, hands-on experience with Snowflake's powerful data warehousing and analytics capabilities.

## Getting Started

To follow along with the session or to experiment with the code on your own, please follow these steps:

1. **Clone the Repository:**
   ```bash
   git clone git@github.com:duspat/snowflake-rwa.git

## Order of Execution 

| SQL Script                          | Description                                                                                                                      |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `db_setup_and_load.sql`             | Initializes the database environment and loads the initial data.                                                               |
| `create_tables.sql`                 | Optionally creates empty tables if you prefer to start with a clean slate.                                                      |
| `create_raw_stream_and_tasks.sql`   | Configures streams on raw tables and sets up tasks to transform raw data into staging tables.                                    |
| `create_staging_stream_and_tasks.sql` | Configures streams on staging tables and sets up tasks to further transform data into a dimensional model.                        |
| `generate_data_utils.sql`           | Provides utility stored procedures to simulate data loading into raw tables and test the complete data flow to dimensional tables.|
