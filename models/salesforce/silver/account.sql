/*
-- Description: Incremental Load Script for Silver Layer - account Table
-- Script Name: silver_account.sql
-- Created on: 15-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the acount table in the Salesforce data pipeline.
-- Data source version: v62.0
-- Change History:
--     15-dec-2025 - Initial creation - Sushil Kompally
*/
{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('account__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('account__salesforce2') }}
)

SELECT *
FROM consolidated