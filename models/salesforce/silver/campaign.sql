/*
-- Description: Incremental Load Script for Silver Layer - campaign Table
-- Script Name: silver_campaign.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the campaign table.
-- Data source version:v62.0
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    materialized='incremental',
    unique_key='campaign_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH consolidated AS (
    SELECT * FROM { ref('campaign__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('campaign__salesforce2') }
)

SELECT *
FROM consolidated