/*
-- Description: Incremental Load Script for Silver Layer - Contact Table
-- Script Name: silver_contact.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the Contact table.
-- Data source version:
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key = 'contact_id',
    incremental_strategy = 'merge',
) }}

WITH consolidated AS (
    SELECT * FROM { ref('contact__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('contact__salesforce2') }
)

SELECT *
FROM consolidated