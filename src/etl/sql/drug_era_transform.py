# pylint: disable=line-too-long
"""
/*********************************************************************************
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
********************************************************************************/
/*******************************************************************************
PURPOSE: Generate Era table (based on conversion script from V4  V5).
last revised: Jun 2017
authors:  Patrick Ryan, Chris Knoll, Anthony Sena, Vojtech Huser
OHDSI-SQL File Instructions
-----------------------------
 1. Set parameter name of schema that contains CDMv4 instance
    (@SOURCE_CDMV4, @SOURCE_CDMV4_SCHEMA)
 2. Set parameter name of schema that contains CDMv5 instance
    ([CDM], [CDM].[CDMSCHEMA])
 3. Run this script through SqlRender to produce a script that will work in your
    source dialect. SqlRender can be found here: https://github.com/OHDSI/SqlRender
 4. Run the script produced by SQL Render on your target RDBDMS.
<RDBMS> File Instructions
-------------------------
 1. This script will hold a number of placeholders for your CDM V4 and CDMV5
    database/schema. In order to make this file work in your environment, you
    should plan to do a global "FIND AND REPLACE" on this file to fill in the
    file with values that pertain to your environment. The following are the
    tokens you should use when doing your "FIND AND REPLACE" operation:
     [CMD]
     [CDM].[CDMSCHEMA]
*********************************************************************************/
/* SCRIPT PARAMETERS */
     -- The target CDMv5 database name
     -- the target CDMv5 database plus schema
--SET search_path TO OHDSI;
DRUG ERA
Note: Eras derived from DRUG_EXPOSURE table, using 30d gap
"""

from typing import Final
from ..models.omopcdm54.registry import TARGET_SCHEMA

SQL: Final[str] = f"""DROP TABLE IF EXISTS cteDrugTarget;
-- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date,
-- or add days supply, or add 1 day to the start date
CREATE TEMP TABLE cteDrugTarget
AS
SELECT
d.DRUG_EXPOSURE_ID
    ,d.PERSON_ID
    ,c.CONCEPT_ID
    ,d.DRUG_TYPE_CONCEPT_ID
    ,DRUG_EXPOSURE_START_DATE
    ,COALESCE(DRUG_EXPOSURE_END_DATE, (DRUG_EXPOSURE_START_DATE + DAYS_SUPPLY*INTERVAL'1 day')
    ,(DRUG_EXPOSURE_START_DATE + 1*INTERVAL'1 day')) AS DRUG_EXPOSURE_END_DATE
    ,c.CONCEPT_ID AS INGREDIENT_CONCEPT_ID
FROM
{TARGET_SCHEMA}.DRUG_EXPOSURE d
INNER JOIN {TARGET_SCHEMA}.CONCEPT_ANCESTOR ca ON ca.DESCENDANT_CONCEPT_ID = d.DRUG_CONCEPT_ID
INNER JOIN {TARGET_SCHEMA}.CONCEPT c ON ca.ANCESTOR_CONCEPT_ID = c.CONCEPT_ID
WHERE c.VOCABULARY_ID = 'RxNorm'
    AND c.CONCEPT_CLASS_ID = 'Ingredient';
/* / */
DROP TABLE IF EXISTS cteEndDates;
/* / */
CREATE TEMP TABLE cteEndDates
AS
SELECT
PERSON_ID
    ,INGREDIENT_CONCEPT_ID
    ,(EVENT_DATE + - 30*INTERVAL'1 day') AS END_DATE -- unpad the end date
FROM
(
    SELECT E1.PERSON_ID
        ,E1.INGREDIENT_CONCEPT_ID
        ,E1.EVENT_DATE
        ,COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL
        ,E1.OVERALL_ORD
    FROM (
        SELECT PERSON_ID
            ,INGREDIENT_CONCEPT_ID
            ,EVENT_DATE
            ,EVENT_TYPE
            ,START_ORDINAL
            ,ROW_NUMBER() OVER (
                PARTITION BY PERSON_ID
                ,INGREDIENT_CONCEPT_ID ORDER BY EVENT_DATE
                    ,EVENT_TYPE
                ) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM (
            -- select the start dates, assigning a row number to each
            SELECT PERSON_ID
                ,INGREDIENT_CONCEPT_ID
                ,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
                ,0 AS EVENT_TYPE
                ,ROW_NUMBER() OVER (
                    PARTITION BY PERSON_ID
                    ,INGREDIENT_CONCEPT_ID ORDER BY DRUG_EXPOSURE_START_DATE
                    ) AS START_ORDINAL
            FROM cteDrugTarget
            UNION ALL
            -- add the end dates with NULL as the row number, padding the end dates by 30
            -- to allow a grace period for overlapping ranges.
            SELECT PERSON_ID
                ,INGREDIENT_CONCEPT_ID
                ,(DRUG_EXPOSURE_END_DATE + 30*INTERVAL'1 day')
                ,1 AS EVENT_TYPE
                ,NULL
            FROM cteDrugTarget
            ) RAWDATA
        ) E1
    INNER JOIN (
        SELECT PERSON_ID
            ,INGREDIENT_CONCEPT_ID
            ,DRUG_EXPOSURE_START_DATE AS EVENT_DATE
            ,ROW_NUMBER() OVER (
                PARTITION BY PERSON_ID
                ,INGREDIENT_CONCEPT_ID ORDER BY DRUG_EXPOSURE_START_DATE
                ) AS START_ORDINAL
        FROM cteDrugTarget
        ) E2 ON E1.PERSON_ID = E2.PERSON_ID
        AND E1.INGREDIENT_CONCEPT_ID = E2.INGREDIENT_CONCEPT_ID
        AND E2.EVENT_DATE <= E1.EVENT_DATE
    GROUP BY E1.PERSON_ID
        ,E1.INGREDIENT_CONCEPT_ID
        ,E1.EVENT_DATE
        ,E1.START_ORDINAL
        ,E1.OVERALL_ORD
    ) E
WHERE 2 * E.START_ORDINAL - E.OVERALL_ORD = 0;
/* / */
DROP TABLE IF EXISTS cteDrugExpEnds;
/* / */
CREATE TEMP TABLE cteDrugExpEnds
AS
SELECT
d.PERSON_ID
    ,d.INGREDIENT_CONCEPT_ID
    ,d.DRUG_TYPE_CONCEPT_ID
    ,d.DRUG_EXPOSURE_START_DATE
    ,MIN(e.END_DATE) AS ERA_END_DATE
FROM
cteDrugTarget d
INNER JOIN cteEndDates e ON d.PERSON_ID = e.PERSON_ID
    AND d.INGREDIENT_CONCEPT_ID = e.INGREDIENT_CONCEPT_ID
    AND e.END_DATE >= d.DRUG_EXPOSURE_START_DATE
GROUP BY d.PERSON_ID
    ,d.INGREDIENT_CONCEPT_ID
    ,d.DRUG_TYPE_CONCEPT_ID
    ,d.DRUG_EXPOSURE_START_DATE;
/* / */
DELETE FROM {TARGET_SCHEMA}.drug_era;
INSERT INTO {TARGET_SCHEMA}.drug_era
SELECT row_number() OVER (
        ORDER BY person_id
        ) AS drug_era_id
    ,person_id
    ,INGREDIENT_CONCEPT_ID
    ,min(DRUG_EXPOSURE_START_DATE) AS drug_era_start_date
    ,ERA_END_DATE
    ,COUNT(*) AS DRUG_EXPOSURE_COUNT
    ,30 AS gap_days
FROM cteDrugExpEnds
GROUP BY person_id
    ,INGREDIENT_CONCEPT_ID
    ,drug_type_concept_id
    ,ERA_END_DATE
;
SELECT COUNT (*) FROM {TARGET_SCHEMA}.drug_era;
"""
