/*
-- Description: Incremental Load Script for Silver Layer - event table
-- Script Name: event_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the tASk_event table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='ACTIVITY_ID',
    incremental_strategy='merge',
) }}

WITH consolidated AS (
    SELECT * FROM { ref('event__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('event__salesforce2') }
)

SELECT *
FROM consolidated