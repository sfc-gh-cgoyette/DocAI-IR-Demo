CREATE OR REPLACE PROCEDURE split_by_even(file_path string, file_name string, dest_stage_name string)
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

CALL split_by_even(build_scoped_file_url(@doc_stage_raw, 'B546_24MA_NORWOOD.pdf'), 'B546_24MA_NORWOOD', '@doc_stage_split' );
