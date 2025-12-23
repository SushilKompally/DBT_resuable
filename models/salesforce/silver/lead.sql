/*
-- Description: Incremental Load Script for Silver Layer - lead table
-- Script Name: lead_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the lead table using
--     reusable macros for metadata, cleanup, and timestamp safety.
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    materialized='incremental',
    unique_key='LEAD_ID',
    incremental_strategy='merge',
) }}

WITH consolidated AS (
    SELECT * FROM { ref('lead__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('lead__salesforce2') }
)

SELECT *
FROM consolidated