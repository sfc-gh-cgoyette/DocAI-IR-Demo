USE DATABASE IR_DOC_AI;
USE SCHEMA RAW_DOC;

CREATE OR REPLACE PROCEDURE split_all_by_every_page(
    stage_name STRING, 
    dest_stage_name STRING
)

RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pypdf2')
HANDLER = 'run_all'
AS
$$
from PyPDF2 import PdfFileReader, PdfWriter
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.file_operation import FileOperation
from io import BytesIO
import os

def run_all(session: Session, stage_name: str, dest_stage_name: str) -> str:

    whole_text = "Success"
    #temp_list = []
    file_list = session.sql(f'LIST {stage_name}').collect()
    for i in file_list:
        file_name = os.path.basename(i['name'])
        file_url = f"{stage_name}/{file_name}"

        #temp_list.append(file_name)

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
        
USE DATABASE IR_DOC_AI;
USE SCHEMA RAW_DOC;

SELECT CURRENT_SCHEMA();

CALL split_all_by_every_page('@BUILD_BOOKS', '@PROCESSED_BUILD_BOOKS');

LIST @split_documents;

