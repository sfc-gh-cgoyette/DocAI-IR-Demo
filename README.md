# DocAI-IR-Demo
Demonstration of document parsing, ingestion, and processing with DocAI in Snowflake

## Pre-processing Documents
In some cases documents need to be split before processing. For example, if you have merged documents such as scans and need to ask the question sets against individual pages, you will first need to split out the pages before processing them with Document AI. Here you have three prewritten stored procedures:

1. Split every page into individual documents
2. Split even pages
3. Split odd pages

## Document AI 
Check for all model builds created
Delete Model Builds
Check usage (credits) of Document AI (Coming Soon)
Join Document usage to query history (Coming Soon)

## Post-Processing Document AI JSON files
- Run predict function with JSON as an output with directory information
- Create flattened table with a list as an array
- Create flattened table with a list flattened
- Restructure tables using a number of lists
- Transform and Filter with JSON lists
- Process all documents within a stage and batch the operation by up to 1000 document every batch
- Process all documents within a stage through a task which checks the resulting table to avoid duplications


### Split document pages every page
Split out every document page individually to process with Document AI

#### Stored Procedure 
make sure have the database, schema and origin and destination stages 

```sql
CREATE OR REPLACE PROCEDURE split_document_by_every_page(
    stage_name STRING, 
    file_name STRING, 
    dest_stage_name STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pypdf2')
HANDLER = 'run'
AS
$$
from PyPDF2 import PdfFileReader, PdfWriter
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.file_operation import FileOperation
from io import BytesIO
import os

def run(session: Session, stage_name: str, file_name: str, dest_stage_name: str) -> str:
    whole_text = "Success"

    # Construct the file URL
    file_url = f"{stage_name}/{file_name}"

    # Retrieve the file from the stage and save it to /tmp/
    get_result = session.file.get(file_url, '/tmp/')
    file_path = os.path.join('/tmp/', file_name)
    
    if not os.path.exists(file_path):
        return f"File {file_name} not found in {stage_name}"

    with open(file_path, 'rb') as f:
        pdf_data = f.read()
        pdf_reader = PdfFileReader(BytesIO(pdf_data))
        num_pages = pdf_reader.getNumPages()

        for page_num in range(num_pages):
            writer = PdfWriter()
            writer.add_page(pdf_reader.getPage(page_num))
                
            # Construct the correct file path for the split page
            batch_filename = f'{file_name}_page_{page_num + 1}.pdf'
            batch_file_path = os.path.join('/tmp/', batch_filename)
            
            # Save each page to a separate PDF file
            with open(batch_file_path, 'wb') as output_file:
                writer.write(output_file)
            
            # Verify the file exists before uploading
            if not os.path.exists(batch_file_path):
                return f"Failed to create the file {batch_filename}"

            # Upload the file to the destination stage
            FileOperation(session).put(
                f"file://{batch_file_path}",
                dest_stage_name,
                auto_compress=False
            )
                
    return whole_text
$$;
```

### To call the stored Procedure after creating it and then list the files in the stage:

```sql
CALL split_document_by_every_page('@BUILD_REPORTS', '1ACII15-100.pdf', '@DOC_STAGE_SPLIT' );
LIST @split_documents;
```

### Split document pages by even numbers
When documents are blank every odd page
When documents only contain pages with relevant content on every even page

#### Stored Procedure 
make sure have the database, schema and origin and destination stages 

```sql
CREATE OR REPLACE PROCEDURE preprocess_pdf(file_path string, file_name string, dest_stage_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pypdf2')
HANDLER = 'run'
AS
$$
from PyPDF2 import PdfFileReader, PdfWriter
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Session, FileOperation
from io import BytesIO
def run(session, file_path, file_name, dest_stage_name):
    whole_text = "Success"
    dest_stage = dest_stage_name
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        input_pdf = PdfFileReader(f)
        writer = PdfWriter()
        # Add only even pages to the writer
        for i in range(1, len(input_pdf.pages), 2):
            writer.add_page(input_pdf.pages[i])
        # Save the even pages to a separate PDF file
        batch_filename = f'//tmp/{file_name}_even.pdf'
        with open(batch_filename, 'wb') as output_file:
            writer.write(output_file)
        FileOperation(session).put("file:///tmp/"+file_name+"_even.pdf", dest_stage, auto_compress = False)
    return whole_text
$$;

```

To call the stored Procedure after creating it:

```sql

CALL preprocess_pdf(build_scoped_file_url(@doc_stage_raw, 'B546_24MA_NORWOOD.pdf'), 'B546_24MA_NORWOOD', '@doc_stage_split' );

```

### Split document pages by odd numbers
When documents are blank every even page
When documents only contain pages with relevant content on every odd page

#### Stored Procedure 
make sure have the database, schema and origin and destination stages 

```sql
CREATE OR REPLACE PROCEDURE preprocess_pdf(file_path string, file_name string, dest_stage_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pypdf2')
HANDLER = 'run'
AS
$$
from PyPDF2 import PdfFileReader, PdfWriter
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Session, FileOperation
from io import BytesIO
def run(session, file_path, file_name, dest_stage_name):
    whole_text = "Success"
    dest_stage = dest_stage_name
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        input_pdf = PdfFileReader(f)
        writer = PdfWriter()
        # Add only odd pages to the writer
        for i in range(0, len(input_pdf.pages), 2):
            writer.add_page(input_pdf.pages[i])
        # Save the odd pages to a separate PDF file
        batch_filename = f'//tmp/{file_name}_odd.pdf'
        with open(batch_filename, 'wb') as output_file:
            writer.write(output_file)
        FileOperation(session).put("file:///tmp/"+file_name+"_odd.pdf", dest_stage, auto_compress = False)
    return whole_text
$$;
```

To call the stored Procedure after creating it:
```sql
CALL preprocess_pdf(build_scoped_file_url(@doc_stage_raw, 'B546_24MA_NORWOOD.pdf'), 'B546_24MA_NORWOOD', '@doc_stage_split' );
```


### See every created Document AI Model Build
```sq'
SHOW INSTANCES OF CLASS SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE;
```

### Delete a specific Document AI Model Build
```sql
DROP INSTANCE NIKOLAI_SNOWFLAKE_TS;
```

### Run predict function with JSON and Directory information in the output

```sql
SELECT 
Relative_path as file_name --https://docs.snowflake.com/en/user-guide/data-load-dirtables-query
, size as file_size
, last_modified
, file_url as snowflake_file_url
, NIKOLAI_MANUAL_TEST!predict(get_presigned_url('@NIKOLAI_TEST_STAGE', RELATIVE_PATH ), 1) as json
from directory(@NIKOLAI_TEST_STAGE)
```

### Create flattened table with list in array

```sql
-- materialize the table with the JSON already unnested (2 options as there are different ways to handle list outputs)
CREATE OR REPLACE TABLE doc_ai_ns.doc_ai_ns.manuals_test_2 AS (
WITH temp as(
SELECT 
Relative_path as file_name --https://docs.snowflake.com/en/user-guide/data-load-dirtables-query
, size as file_size
, last_modified
, file_url as snowflake_file_url
, NIKOLAI_MANUAL_TEST!predict(get_presigned_url('@NIKOLAI_TEST_STAGE', RELATIVE_PATH ), 1) as json
from directory(@NIKOLAI_TEST_STAGE)
)

SELECT
file_name
, file_size
, last_modified
, snowflake_file_url
, json:__documentMetadata.ocrScore::FLOAT AS ocrScore
, f.value:score::FLOAT AS inspection_date_score
, f.value:value::STRING AS inspection_date_value
, g.value:score::FLOAT AS inspection_grade_score
, g.value:value::STRING AS inspection_grade_value
, h.value:score::FLOAT AS inspection_recommendation_score
, h.value:value::STRING AS inspection_recommendation_value
, i.value:score::FLOAT AS inspector_score
, i.value:value::STRING AS inspector_value
, ARRAY_TO_STRING(ARRAY_AGG(j.value:value::STRING), ', ') AS list_of_units --if you want to comma delimit the list
FROM doc_ai_ns.doc_ai_ns.manuals_test_table
, LATERAL FLATTEN(input => json:inspection_date) f
, LATERAL FLATTEN(input => json:inspection_grade) g
, LATERAL FLATTEN(input => json:inspection_recommendation) h
, LATERAL FLATTEN(input => json:inspector) i
, LATERAL FLATTEN(input => json:list_of_units) j --if you want to comma delimit the list
GROUP BY ALL
;
```

### Create flattened table with a list flattened out

```sql
-- materialize the table with the JSON already unnested (2 options as there are different ways to handle list outputs)
CREATE OR REPLACE TABLE doc_ai_ns.doc_ai_ns.manuals_test_2 AS (
WITH temp as(
SELECT 
Relative_path as file_name --https://docs.snowflake.com/en/user-guide/data-load-dirtables-query
, size as file_size
, last_modified
, file_url as snowflake_file_url
, NIKOLAI_MANUAL_TEST!predict(get_presigned_url('@NIKOLAI_TEST_STAGE', RELATIVE_PATH ), 1) as json
from directory(@NIKOLAI_TEST_STAGE)
)

, first_flatten AS (
SELECT
file_name
, file_size
, last_modified
, snowflake_file_url
, json:__documentMetadata.ocrScore::FLOAT AS ocrScore
, f.value:score::FLOAT AS inspection_date_score
, f.value:value::STRING AS inspection_date_value
, g.value:score::FLOAT AS inspection_grade_score
, g.value:value::STRING AS inspection_grade_value
, h.value:score::FLOAT AS inspection_recommendation_score
, h.value:value::STRING AS inspection_recommendation_value
, i.value:score::FLOAT AS inspector_score
, i.value:value::STRING AS inspector_value
, ARRAY_TO_STRING(ARRAY_AGG(j.value:value::STRING), ', ') AS list_of_units --if you want to comma delimit the list
FROM doc_ai_ns.doc_ai_ns.manuals_test_table
, LATERAL FLATTEN(input => json:inspection_date) f
, LATERAL FLATTEN(input => json:inspection_grade) g
, LATERAL FLATTEN(input => json:inspection_recommendation) h
, LATERAL FLATTEN(input => json:inspector) i
GROUP BY ALL
)

, second_flatten AS ( 
SELECT 
file_name
, j.value:score::FLOAT AS unit_score
, j.value:value::STRING AS unit_value
FROM temp
, LATERAL FLATTEN(input => json:list_of_units) j
)

SELECT 
a.*
, b.unit_score
, b.unit_value
FROM first_flatten a
LEFT JOIN second_flatten b ON a.file_name = b.file_name
)
;
```

### Restructure tables using a number of lists

```sql
CREATE TABLE doc_ai_ns.doc_ai_ns.test_table_2 (
    result VARIANT
);


INSERT INTO doc_ai_ns.doc_ai_ns.test_table_2 (result)
SELECT PARSE_JSON('{
  "order_number": [
    {
      "score": 1,
      "value": "0067891"
    },
    {
      "score": 1,
      "value": "0067892"
    },
    {
      "score": 1,
      "value": "0067893"
    },
    {
      "score": 1,
      "value": "0067894"
    }
  ],
  "company": [
    {
      "score": 1,
      "value": "TECH SOLUTIONS INC"
    }
  ],
  "due_date": [
    {
      "score": 0.98,
      "value": "September 15, 2024"
    }
  ],
  "list_number": [
    {
      "score": 1,
      "value": "2024 NS 004567"
    }
  ],
  "item_codes": [
    {
      "score": 1,
      "value": "X123"
    },
    {
      "score": 1,
      "value": "X456"
    },
    {
      "score": 1,
      "value": "X789"
    },
    {
      "score": 1,
      "value": "X012"
    }
  ],
  "supplier": [
    {
      "score": 1,
      "value": "GLOBAL SUPPLY CO."
    }
  ],
  "cost": [
    {
      "score": 1,
      "value": "250.50"
    },
    {
      "score": 1,
      "value": "350.75"
    },
    {
      "score": 1,
      "value": "150.20"
    },
    {
      "score": 1,
      "value": "450.99"
    }
  ],
  "total_due": [
    {
      "score": 1,
      "value": "1,202.44"
    }
  ],
  "city_name": [
    {
      "score": 1,
      "value": "San Francisco"
    }
  ],
  "inventory_number": [
    {
      "score": 0.995,
      "value": "1HGBH41JXMN109186"
    },
    {
      "score": 0.995,
      "value": "1HGBH41JXMN109187"
    },
    {
      "score": 0.995,
      "value": "1HGBH41JXMN109188"
    },
    {
      "score": 0.995,
      "value": "1HGBH41JXMN109189"
    }
  ],
  "__documentMetadata": {
    "ocrScore": 0.921
  }
}');

WITH
-- Extract multi-value lists
  order_number AS (
    SELECT
      seq4() AS seq,
      value:value::STRING AS order_number
    FROM test_table_2
    , LATERAL FLATTEN(input => result:order_number) 
  ),

  item_codes AS (
    SELECT
      seq4() AS seq,
      value:value::STRING AS item_codes
    FROM test_table_2
    ,LATERAL FLATTEN(input => result:item_codes)
  ),

  cost AS (
    SELECT
      seq4() AS seq,
      value:value::STRING AS cost
    FROM test_table_2
    ,LATERAL FLATTEN(input => result:cost) 
  ),

  inventory_number AS (
    SELECT
      seq4() AS seq,
      value:value::STRING AS inventory_number
    FROM test_table_2
    ,LATERAL FLATTEN(input => result:inventory_number) 
  ),

  -- Extract single-value attributes 
  single_value_attributes AS (
    SELECT
      a.value:value::STRING AS company,
      b.value:value::STRING AS due_date,
      c.value:value::STRING AS list_Number,
      d.value:value::STRING AS supplier,
      e.value:value::STRING AS total_Due,
      f.value:value::STRING AS City_Name
    FROM test_table_2
    , LATERAL FLATTEN(input => result:company) a
    , LATERAL FLATTEN(input => result:due_date) b
    , LATERAL FLATTEN(input => result:list_number) c
    , LATERAL FLATTEN(input => result:supplier) d
    , LATERAL FLATTEN(input => result:total_due) e
    , LATERAL FLATTEN(input => result:city_name) f
  )

--merge
  SELECT
  b.order_number,
  s.company,
  s.due_date,
  s.list_number,
  r.item_codes,
  s.supplier,
  t.cost,
  s.total_due,
  s.city_name,
  v.inventory_number
FROM
  order_number b
  LEFT JOIN item_codes r ON b.seq = r.seq
  LEFT JOIN cost t ON b.seq = t.seq
  LEFT JOIN inventory_number v ON b.seq = v.seq
  CROSS JOIN single_value_attributes s
ORDER BY
  b.seq;
;
```


### Transform and Filter with JSON lists
- Higher Order Functions https://medium.com/snowflake/snowflake-supports-higher-order-functions-dfa4b7682f7a 
- TRANSFORM - https://docs.snowflake.com/en/sql-reference/functions/transform 
- FILTER - https://docs.snowflake.com/en/sql-reference/functions/filter 

### Process all documents within a stage and batch the operation by up to 1000 document every batch

```sql
CREATE OR REPLACE PROCEDURE batch_prediction(model_name VARCHAR, model_version INTEGER, stage_name VARCHAR, result_table_name VARCHAR, batch_size INTEGER)
RETURNS TABLE(file_name VARCHAR(100), prediction VARCHAR)
LANGUAGE SQL
AS
  DECLARE
    maximum_count_query VARCHAR DEFAULT 'select COUNT(distinct metadata$filename) as no from @' || :stage_name;
    maximum_count RESULTSET;
    res RESULTSET;
    stage_size INTEGER;
    i INTEGER DEFAULT 0;
    presigned_url_table_name VARCHAR DEFAULT 'presigned_urls_' || (SELECT randstr(15, random()));
    query_create_table VARCHAR DEFAULT 'create or replace table ' || :result_table_name || '(file_name VARCHAR(100), prediction VARCHAR)';
    query_create_presigned VARCHAR;
    query_insert VARCHAR DEFAULT 'insert into ' || :result_table_name || '(file_name, prediction) select file_name, ' || :model_name || '!predict(presigned_url, ' || :model_version || ') from ' || :presigned_url_table_name;
    query_result VARCHAR DEFAULT 'select * from ' || :result_table_name;
  BEGIN
    res := (EXECUTE IMMEDIATE :query_create_table);
    maximum_count := (EXECUTE IMMEDIATE :maximum_count_query);
    FOR j IN maximum_count DO
      stage_size := j.no;
      WHILE (i < stage_size) DO
        query_create_presigned := 'create or replace temporary table ' || :presigned_url_table_name || '(file_name, presigned_url) as select distinct split_part(metadata$filename, \'/\',-1) as fn, get_presigned_url(@' || :stage_name || ', fn) from @' || :stage_name || ' limit ' || :batch_size || 'offset ' || i;
        res := (EXECUTE IMMEDIATE :query_create_presigned);
        res := (EXECUTE IMMEDIATE :query_insert);
        i := i + :batch_size;
      END WHILE;
    END FOR;
    res := (EXECUTE IMMEDIATE :query_result);
    RETURN TABLE(res);
  END;
```

```
CALL batch_prediction('NIKOLAI_CONTRACT_TEST', 2, 'CONTRACTS_TEST', 'contract_test_2', 20);
```

### Process all documents within a stage through a task which checks the resulting table to avoid duplications

```sql
-- create the output table, important part is to have the `relative_path` there
create table if not exists stage_results(relative_path varchar, res varchar);

--create the task
CREATE OR REPLACE TASK process_from_stage
  WAREHOUSE = WH1
  SCHEDULE = '1 minute'
  COMMENT = 'Process the next chuck of files from the stage'
AS
INSERT INTO stage_results (
  SELECT
    RELATIVE_PATH,
    -- use your build name and stage
    BF_NOPUB!PREDICT(GET_PRESIGNED_URL(@db1.schema1.PERF_100, relative_path)) as res  
  FROM 
    -- use your stage
    DIRECTORY(@db1.schema1.PERF_100)
  WHERE 
     RELATIVE_PATH not in (
       select RELATIVE_PATH from stage_results
     ) 
  ORDER BY LAST_MODIFIED ASC  
  -- the current max limit is 20
  -- keep in mind that the default limit for TASK execution is 1 hour only!
  LIMIT 20
); 
```
