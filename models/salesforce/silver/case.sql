/*
-- Description: Incremental Load Script for Silver Layer - case Table
-- Script Name: silver_case.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the case table.
-- Data source version:
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='case_id',
    incremental_strategy='merge',
) }}

WITH consolidated AS (
    SELECT * FROM { ref('case__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('case__salesforce2') }
)

SELECT *
FROM consolidated