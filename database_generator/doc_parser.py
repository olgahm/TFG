from glob import glob
from io import BytesIO
from unidecode import unidecode
import re
import requests
import string
import subprocess
import os
import traceback
import pytesseract
import glob
import os
from wand.image import Image as WImage
from PIL import ImageEnhance, ImageFilter
from PIL import Image as Img

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage
from database_generator.info_storage import item_to_database
from database_generator.info_storage import my_logger as doc_logger


def parse_docs(bids, doc_links, formats, process_id):
    for index, url in enumerate(doc_links):
        if 'pdf' in formats[index]:
            get_pdf_text(bids[index], url, process_id)


def get_pdf_text(bid_name, url, process_id):
    doc_logger.debug(f'Process-{process_id}. Extracting text from {bid_name}')
    doc_metadata = {'id_licitacion': bid_name}
    file_name = f'document_parsing/PDF/{re.sub(f"[./{string.punctuation}]", "_", bid_name).strip()}.pdf'
    print(url)
    r = requests.get(url)
    with open(file_name, 'wb') as fopen:
        fopen.write(r.content)
    doc_content = pdf2txt(file_name, bid_name, doc_metadata)
    if doc_content:
        doc_metadata['texto_pliego'] = doc_content
        item_to_database(doc_metadata, process_id)


def pdf2txt(path, bid_name, doc_metadata):
    try:
        final_text = extract_pdf_txt(path)
    except:
        return ''
    final_text = unidecode(final_text)
    final_text = re.sub(f"""[%©&!.º*°●§‘'’<«>~-—…|“\"?(),$/{string.punctuation}]""", '', final_text)
    final_text = re.sub("[	 \n]+", ' ', final_text).strip()
    final_text = ' '.join([word for word in final_text.split() if len(word) > 1])
    if not final_text or final_text.isspace() and final_text is not None or len(final_text.split()) < 10:
        try:
            doc_metadata['formato_pliego'] = 'pdf_ocr'
            with WImage(filename=path, resolution=350) as img:
                img.save(filename=f'{re.sub(f"[./{string.punctuation}]", "_", bid_name).strip()}.jpeg')
            text = str()
            for file in glob.glob('*.jpeg'):
                with Img.open(file) as im:
                    im = im.filter(ImageFilter.MedianFilter())
                    enhancer = ImageEnhance.Contrast(im)
                    im = enhancer.enhance(2)
                    im = im.convert('1')
                    im.save(file)
                text += pytesseract.image_to_string(Img.open(file), lang='spa')
                os.remove(file)
            print(text)
            final_text = re.sub(f"""[%©&!.º*°●§‘'’<«>~-—…“\"|?(),$/{string.punctuation}]""", '', unidecode(text))
            final_text = re.sub("[ \n]+", ' ', final_text).strip()
            final_text = ' '.join([word for word in final_text.split() if len(word) > 1])
        except:
            print(traceback.format_exc())
            final_text = ''
    os.remove(path)
    return final_text


def extract_pdf_txt(path):
    """Implementation of pdfminer's command pdf2txt

    :param path: Path to PDF document to be parsed
    :return: Extracted text
    """
    manager = PDFResourceManager()
    retstr = BytesIO()
    layout = LAParams(all_texts=True)
    device = TextConverter(manager, retstr, laparams=layout)
    filepath = open(path, 'rb')
    interpreter = PDFPageInterpreter(manager, device)
    text = ''
    try:
        for page in PDFPage.get_pages(filepath, check_extractable=True):
            interpreter.process_page(page)

        text = retstr.getvalue()
        device.close()
        retstr.close()
        filepath.close()
        text = text.decode('utf-8')
    except:
        filepath.close()
        text = ''
    finally:
        return text
