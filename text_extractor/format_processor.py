import re
import zipfile
from io import BytesIO

import rarfile
import requests
from bs4 import BeautifulSoup

from config import FILE_EXTENSIONS_TO_PARSE, FILE_EXTENSIONS_TO_IGNORE, FINAL_CONTENT_TYPES_TO_PARSE, FINAL_CONTENT_TYPES_TO_IGNORE, text_logger
from urllib.parse import urlparse
import sys


def html_to_doc(url, response, db_conn):
    """Function to analyze html responses depending on the domain.
    At this point, we downloaded the document and the content type of the response is an html.
    This function will analyze the content of the html to see if this is an actual document or a mid page to redirect us to the actual document

    :param response:
    :return:
    """
    # Check if this response corresponds to PCSP's mid page redirecting to actual document
    text_logger.debug(f'Html detected. Browsing though {url}...')
    parsed_uri = urlparse(response.url)
    domain = f'{parsed_uri.scheme}://{parsed_uri.netloc}/'
    domain_function = html_processing_by_domain_map.get(domain)
    if domain_function is not None:
        content_type, buffer = domain_function(url=url, response=response, db_conn=db_conn)
        if content_type is None:
            return None
        elif content_type not in FINAL_CONTENT_TYPES_TO_PARSE and content_type not in FINAL_CONTENT_TYPES_TO_IGNORE:
            if content_type_parsing_functions.get(content_type) is not None:
                if 'html' in content_type:
                    print(url)
                    print('Still have html, what do we do with it?')
                    return None
                text_logger.debug(f'Found {content_type} in html after analyzing it. Need further processing...')
                print(f'Found {content_type} in html after analyzing it. Need further processing...')
                print(url)
                documents = content_type_parsing_functions[content_type](buffer)
                if documents:
                    print('Docs found')
                    return documents
                else:
                    print('No docs to return')
                    return None

            else:
                text_logger.debug(f'Found {content_type} in html after analyzing it. Need further processing?')
                print(url)
                print(f'Found {content_type} in html after analyzing it. Need further processing?')
                return None
        elif content_type in FINAL_CONTENT_TYPES_TO_IGNORE:
            print('Got content type to ignore. Deleting from database...')
            text_logger.debug('Got content type to ignore. Deleting from database...')
            db_conn.deleteRowTables('docs', f"doc_url='{url}'")
        else:
            return [buffer]

        # sys.exit(0)
        # print(f'Ignored URL due to unknown html structure: {response.url}')
    else:
        print(f'What do we do with this domain: {domain}. This is the url: {url}')
        return None
    # url_parsing_function =
    # soup = BeautifulSoup(response.content, 'html.parser')
    # doc_link = soup.find('meta')
    # if doc_link is not None:
    #
    #     if doc_format in content_types_to_parse:
    #         if 'zip' in doc_format:
    #             documents = zip_to_docs(response.content)
    #         elif 'rar' in doc_format:
    #             documents = rar_to_docs(response.content)
    #         else:
    #             documents = [response.content]
    #         return documents
    #     else:
    #         print(f'Unknown content-type {doc_format}')
    #         return None
    # else:
    #     If not pcsp, think what to do next
    # print('Unknown html structure. Exiting...')
    # return None


def parse_pcsp_html(url, response, db_conn):
    soup = BeautifulSoup(response.content, 'html.parser')
    doc_link = soup.find('meta')
    if doc_link is not None:
        relative_link = doc_link['content']
        relative_link = re.search("\\'(.*)\\'", relative_link).group(1)
        abs_link = f'https://contrataciondelestado.es{relative_link}'
        response = requests.get(abs_link)
        doc_format = response.headers['Content-Type'].lower()
        return doc_format, response.content
    elif 'Documento no accesible' in response.content.decode('utf8'):
        print(f'Document in {url} no longer accessible. Deleting from database...')
        text_logger.debug(f'Document in {url} no longer accessible. Deleting from database...')
        db_conn.deleteRowTables('docs', f"doc_url='{url}'")
    else:
        print(url)
        print("Study structure of pscp's url received")
    return None, None





# def rar_to_docs(root):
#     with rarfile.RarFile(BytesIO(root)) as rar_file:
#         documents = list()
#         for fileinfo in rar_file.infolist():
#             if fileinfo.filename[-1] != '/':
#                 doc_format = fileinfo.filename.split('.')[-1].lower()
#                 if doc_format in FILE_EXTENSIONS_TO_PARSE and doc_format not in FILE_EXTENSIONS_TO_IGNORE:
#                     with rar_file.open(fileinfo) as file:
#                         documents.append(file.read())
#                 else:
#                     print(f'Unkwnown format {doc_format}')
#     return documents
#
#
# def zip_to_docs(root):
#     with zipfile.ZipFile(BytesIO(root)) as zip_file:
#         documents = list()
#         for fileinfo in zip_file.infolist():
#             if fileinfo.filename[-1] != '/':
#                 doc_format = fileinfo.filename.split('.')[-1].lower()
#                 if doc_format in FILE_EXTENSIONS_TO_PARSE and doc_format not in FILE_EXTENSIONS_TO_IGNORE:
#                     try:
#                         with zip_file.open(fileinfo) as file:
#                             documents.append(file.read())
#                     except:
#                         print(f'Cannot open file {fileinfo.filename} to get text buffer. Skipping to next one...')
#                 elif doc_format in FILE_EXTENSIONS_TO_IGNORE:
#                     print(f'Ignored content type {doc_format}')
#                 else:
#                     print(f'filename {fileinfo.filename}')
#                     print(f'Unkwnown format {doc_format} in zip')
#     return documents


content_type_parsing_functions = {'text/html': html_to_doc}
html_processing_by_domain_map = {
    'ignore': ['https://apps.euskadi.eus/', 'https://contractaciopublica.gencat.cat'],
    'https://contrataciondelestado.es:443/': parse_pcsp_html,
    'https://contrataciondelestado.es/': parse_pcsp_html}