"""condition era transform"""

# /*********************************************************************************
# # Copyright 2017 Observational Health Data Sciences and Informatics
# #
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #     http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# ********************************************************************************/
# /*******************************************************************************
# PURPOSE: Generate Era table (based on conversion script from V4  V5).
# last revised: Jun 2017
# authors:  Patrick Ryan, Chris Knoll, Anthony Sena, Vojtech Huser
# OHDSI-SQL File Instructions
# -----------------------------
#  1. Set parameter name of schema that contains CDMv4 instance
#     (@SOURCE_CDMV4, @SOURCE_CDMV4_SCHEMA)
#  2. Set parameter name of schema that contains CDMv5 instance
#     ([CDM], [CDM].[CDMSCHEMA])
#  3. Run this script through SqlRender to produce a script that will work in your
#     source dialect. SqlRender can be found here: https://github.com/OHDSI/SqlRender
#  4. Run the script produced by SQL Render on your target RDBDMS.
# <RDBMS> File Instructions
# -------------------------
#  1. This script will hold a number of placeholders for your CDM V4 and CDMV5
#     database/schema. In order to make this file work in your environment, you
#     should plan to do a global "FIND AND REPLACE" on this file to fill in the
#     file with values that pertain to your environment. The following are the
#     tokens you should use when doing your "FIND AND REPLACE" operation:
#      [CMD]
#      [CDM].[CDMSCHEMA]
# *********************************************************************************/
# /* SCRIPT PARAMETERS */
#      -- The target CDMv5 database name
#      -- the target CDMv5 database plus schema
# --SET search_path TO OHDSI;
# CONDITION ERA
# Note: Eras derived from CONDITION_OCCURRENCE table, using 30d gap
# Copyright 2017 Observational Health Data Sciences and Informatics
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ********************************************************************************/
# /*******************************************************************************
# PURPOSE: Generate Era table (based on conversion script from V4  V5).
# last revised: Jun 2017
# authors:  Patrick Ryan, Chris Knoll, Anthony Sena, Vojtech Huser
# OHDSI-SQL File Instructions
# -----------------------------
#  1. Set parameter name of schema that contains CDMv4 instance
#     (@SOURCE_CDMV4, @SOURCE_CDMV4_SCHEMA)
#  2. Set parameter name of schema that contains CDMv5 instance
#     ([CDM], [CDM].[CDMSCHEMA])
#  3. Run this script through SqlRender to produce a script that will work in your
#     source dialect. SqlRender can be found here: https://github.com/OHDSI/SqlRender
#  4. Run the script produced by SQL Render on your target RDBDMS.
# <RDBMS> File Instructions
# -------------------------
#  1. This script will hold a number of placeholders for your CDM V4 and CDMV5
#     database/schema. In order to make this file work in your environment, you
#     should plan to do a global "FIND AND REPLACE" on this file to fill in the
#     file with values that pertain to your environment. The following are the
#     tokens you should use when doing your "FIND AND REPLACE" operation:
#      [CMD]
#      [CDM].[CDMSCHEMA]
# *********************************************************************************/
# /* SCRIPT PARAMETERS */
#      -- The target CDMv5 database name
#      -- the target CDMv5 database plus schema
# --SET search_path TO OHDSI;
# CONDITION ERA
# Note: Eras derived from CONDITION_OCCURRENCE table, using 30d gap
from typing import Final
from ..models.omopcdm54.registry import TARGET_SCHEMA

SQL: Final = f"""
/****
CONDITION ERA
Note: Eras derived from CONDITION_OCCURRENCE table, using 30d gap
 ****/
DROP TABLE IF EXISTS condition_era_phase_1;
/* / */
DROP TABLE IF EXISTS cteConditionTarget;
/* / */
-- create base eras from the concepts found in condition_occurrence
CREATE TEMP TABLE cteConditionTarget
AS
SELECT
co.PERSON_ID
    ,co.condition_concept_id
    ,co.CONDITION_START_DATE
    ,COALESCE(co.CONDITION_END_DATE, (CONDITION_START_DATE + 1*INTERVAL'1 day')) AS CONDITION_END_DATE
FROM
{TARGET_SCHEMA}.CONDITION_OCCURRENCE co;
/* / */
DROP TABLE IF EXISTS cteCondEndDates;
/* / */
CREATE TEMP TABLE cteCondEndDates
AS
SELECT
PERSON_ID
    ,CONDITION_CONCEPT_ID
    ,(EVENT_DATE + - 30*INTERVAL'1 day') AS END_DATE -- unpad the end date
FROM
(
    SELECT E1.PERSON_ID
        ,E1.CONDITION_CONCEPT_ID
        ,E1.EVENT_DATE
        ,COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL
        ,E1.OVERALL_ORD
    FROM (
        SELECT PERSON_ID
            ,CONDITION_CONCEPT_ID
            ,EVENT_DATE
            ,EVENT_TYPE
            ,START_ORDINAL
            ,ROW_NUMBER() OVER (
                PARTITION BY PERSON_ID
                ,CONDITION_CONCEPT_ID ORDER BY EVENT_DATE
                    ,EVENT_TYPE
                ) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM (
            -- select the start dates, assigning a row number to each
            SELECT PERSON_ID
                ,CONDITION_CONCEPT_ID
                ,CONDITION_START_DATE AS EVENT_DATE
                ,- 1 AS EVENT_TYPE
                ,ROW_NUMBER() OVER (
                    PARTITION BY PERSON_ID
                    ,CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE
                    ) AS START_ORDINAL
            FROM cteConditionTarget
            UNION ALL
            -- pad the end dates by 30 to allow a grace period for overlapping ranges.
            SELECT PERSON_ID
                ,CONDITION_CONCEPT_ID
                ,(CONDITION_END_DATE + 30*INTERVAL'1 day')
                ,1 AS EVENT_TYPE
                ,NULL
            FROM cteConditionTarget
            ) RAWDATA
        ) E1
    INNER JOIN (
        SELECT PERSON_ID
            ,CONDITION_CONCEPT_ID
            ,CONDITION_START_DATE AS EVENT_DATE
            ,ROW_NUMBER() OVER (
                PARTITION BY PERSON_ID
                ,CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE
                ) AS START_ORDINAL
        FROM cteConditionTarget
        ) E2 ON E1.PERSON_ID = E2.PERSON_ID
        AND E1.CONDITION_CONCEPT_ID = E2.CONDITION_CONCEPT_ID
        AND E2.EVENT_DATE <= E1.EVENT_DATE
    GROUP BY E1.PERSON_ID
        ,E1.CONDITION_CONCEPT_ID
        ,E1.EVENT_DATE
        ,E1.START_ORDINAL
        ,E1.OVERALL_ORD
    ) E
WHERE (2 * E.START_ORDINAL) - E.OVERALL_ORD = 0;
/* / */
DROP TABLE IF EXISTS cteConditionEnds;
/* / */
CREATE TEMP TABLE cteConditionEnds
AS
SELECT
c.PERSON_ID
    ,c.CONDITION_CONCEPT_ID
    ,c.CONDITION_START_DATE
    ,MIN(e.END_DATE) AS ERA_END_DATE
FROM
cteConditionTarget c
INNER JOIN cteCondEndDates e ON c.PERSON_ID = e.PERSON_ID
    AND c.CONDITION_CONCEPT_ID = e.CONDITION_CONCEPT_ID
    AND e.END_DATE >= c.CONDITION_START_DATE
GROUP BY c.PERSON_ID
    ,c.CONDITION_CONCEPT_ID
    ,c.CONDITION_START_DATE;
/* / */
DELETE from {TARGET_SCHEMA}.condition_era;
INSERT INTO {TARGET_SCHEMA}.condition_era (
    condition_era_id
    ,person_id
    ,condition_concept_id
    ,condition_era_start_date
    ,condition_era_end_date
    ,condition_occurrence_count
    )
SELECT row_number() OVER (
        ORDER BY person_id
        ) AS condition_era_id
    ,person_id
    ,CONDITION_CONCEPT_ID
    ,min(CONDITION_START_DATE) AS CONDITION_ERA_START_DATE
    ,ERA_END_DATE AS CONDITION_ERA_END_DATE
    ,COUNT(*) AS CONDITION_OCCURRENCE_COUNT
FROM cteConditionEnds
GROUP BY person_id
    ,CONDITION_CONCEPT_ID
    ,ERA_END_DATE;
SELECT COUNT (*) FROM {TARGET_SCHEMA}.condition_era;
"""
