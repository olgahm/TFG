import re
import zipfile
from io import BytesIO

import rarfile
import requests
from bs4 import BeautifulSoup

from config import formats_to_parse, formats_to_ignore


def html_to_doc(response):
    # Check if this response corresponds to PCSP's mid page redirecting to actual document
    soup = BeautifulSoup(response, 'html.parser')
    doc_link = soup.find('meta')
    if doc_link is not None:
        relative_link = doc_link['content']
        relative_link = re.search("\\'(.*)\\'", relative_link).group(1)
        abs_link = f'https://contrataciondelestado.es{relative_link}'
        response = requests.get(abs_link)
        doc_format = response.headers['Content-Type'].lower()
        if doc_format in formats_to_parse and doc_format not in formats_to_ignore:
            if 'zip' in doc_format:
                documents = zip_to_docs(response.content)
            elif 'rar' in doc_format:
                documents = rar_to_docs(response.content)
            else:
                documents = [response.content]
            return documents
        else:
            print(f'Unknown content-type {format}')
            return None
    else:
        # If not pcsp, think what to do next
        print('Unknown html structure. Exiting...')
        return None


def rar_to_docs(root):
    with rarfile.RarFile(BytesIO(root)) as rar_file:
        documents = list()
        for fileinfo in rar_file.infolist():
            if fileinfo.filename[-1] != '/':
                doc_format = fileinfo.filename.split('.')[-1].lower()
                if doc_format in formats_to_parse and doc_format not in formats_to_ignore:
                    with rar_file.open(fileinfo) as file:
                        documents.append(file.read())
                else:
                    print(f'Unkwnown format {doc_format}')
    return documents


def zip_to_docs(root):
    with zipfile.ZipFile(BytesIO(root)) as zip_file:
        documents = list()
        for fileinfo in zip_file.infolist():
            if fileinfo.filename[-1] != '/':
                doc_format = fileinfo.filename.split('.')[-1].lower()
                if doc_format in formats_to_parse and doc_format not in formats_to_ignore:
                    with zip_file.open(fileinfo) as file:
                        documents.append(file.read())
                else:
                    print(f'Unkwnown format {doc_format}')
    return documents


format_parsing = {'text/html': html_to_doc}
