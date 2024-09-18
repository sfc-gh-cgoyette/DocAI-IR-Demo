-- assume the accountadmin role
USE ROLE accountadmin;

-- create the IR_doc_ai database
CREATE OR REPLACE DATABASE IR_doc_ai;

-- create the raw_doc schema
CREATE OR REPLACE SCHEMA IR_doc_ai.raw_doc;

-- create the doc_ai stage
CREATE OR REPLACE STAGE IR_doc_ai.raw_doc.doc_ai
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION =  (TYPE = 'SNOWFLAKE_SSE');

-- create the build_books stage
CREATE OR REPLACE STAGE IR_doc_ai.raw_doc.build_books
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION =  (TYPE = 'SNOWFLAKE_SSE');

-- create additional stage for processed docs
CREATE OR REPLACE STAGE IR_doc_ai.raw_doc.processed_build_books
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION =  (TYPE = 'SNOWFLAKE_SSE');

-- create the doc_ai warehouse
CREATE OR REPLACE WAREHOUSE doc_ai
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'document ai warehouse';

-- create the IR_doc_ai role
CREATE OR REPLACE ROLE IR_doc_ai;

-- grant document ai privileges
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE IR_doc_ai;

-- grant doc_ai warehouse privileges
GRANT USAGE, OPERATE ON WAREHOUSE doc_ai TO ROLE IR_doc_ai;

-- grant IR_doc_ai database privileges
GRANT ALL ON DATABASE IR_doc_ai TO ROLE IR_doc_ai;
GRANT ALL ON SCHEMA IR_doc_ai.raw_doc TO ROLE IR_doc_ai;
GRANT CREATE STAGE ON SCHEMA IR_doc_ai.raw_doc TO ROLE IR_doc_ai;
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA IR_doc_ai.raw_doc TO ROLE IR_doc_ai;
GRANT ALL ON ALL STAGES IN SCHEMA IR_doc_ai.raw_doc TO ROLE IR_doc_ai;
