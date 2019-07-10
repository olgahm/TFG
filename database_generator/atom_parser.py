from lxml import etree
import requests
import re
import iso8601
import pytz
# from database_generator.db_helpers import is_stored
# from database_generator.db_helpers import get_data_from_table
from helpers import insert_or_update_records
from config import db_logger
from unidecode import unidecode
from datetime import datetime, timedelta
from collections import Counter
import sys

bid_schema = {"bid_uri": None, "title": None, "summary": None, "link": None, "deleted_at_offset": None,
              "last_updated_offset": None, "bid_status": None, "id_expediente": None, "duracion": None,
              "tipo_contrato": None, "subtipo_contrato": None, "comunidad_ejecucion": None, "ciudad_ejecucion": None,
              "zona_postal_lugar_ejecucion": None, "pais_ejecucion": None, "tipo_procedimiento": None,
              "sistema_contratacion": None, "tipo_tramitacion": None, "presentacion_oferta": None, "idioma": None,
              "organo_de_contratacion": None, "proveedor_pliegos": None, "receptor_ofertas": None,
              "deadline_descripcion": None, "formula_revision_precio": None, "programa_financiacion": None,
              "titulo_habilitante": None, "descripcion_requisitos_participacion": None,
              "clasificacion_empresarial_solicitada": None, "codigo_clasificacion_empresarial_solicitada": None,
              "condiciones_admision": None, "objeto_subcontratacion": None, "id_sobre": None,
              "tipo_documento_sobre": None, "descripcion_preparacion_oferta": None, "condiciones_adjudicacion": None,
              "criterio_limitacion_numero_candidatos": None, "codigo_justificacion_proceso_extraordinario": None,
              "descripcion_justificacion_proceso_extraordinario": None, "deleted_at": None, "last_updated": None,
              "plazo_pliegos": None, "plazo_presentacion": None, "valor_estimado": None,
              "presupuesto_base_sin_impuestos": None, "presupuesto_base_total": None,
              "porcentaje_maximo_subcontratacion": None, "ponderacion_adjudicacion": None, "fecha_inicio": None,
              "fecha_fin": None, "curriculum_requerido": None, "admision_variantes": None, "candidatos_esperados": None,
              "candidatos_maximos": None, "candidatos_minimos": None}
doc_schema = {"doc_id": None, "doc_url": None, "bid_id": None, "doc_type": None, "doc_hash": None}
org_schema = {"nombre": None, "tipo_organismo": None, "uri": None, "id": None, "direccion": None, "cp": None,
              "ciudad": None, "pais": None, "nombre_contacto": None, "telefono_contacto": None, "fax_contacto": None,
              "email_contacto": None, "razon_social": None, "bid_id": None}

event_schema = {"bid_id": None, "tipo": None, "id_evento": None, "descripcion": None, "lugar": None, "direccion": None,
                "cp": None, "ciudad": None, "pais": None, "fecha": None}

winner_schema = {"bid_id": None, "resultado": None, "adjudicatario": None, "id_contrato": None,
                 "objeto_subcontratacion": None, "id_lote": None, "motivacion": None, "importe_sin_impuestos": None,
                 "importe_con_impuestos": None, "maxima_oferta_recibida": None, "minima_oferta_recibida": None,
                 "porcentaje_subcontratacion": None, "numero_participantes": None, "fecha_adjudicacion": None,
                 "fecha_formalizacion": None, "fecha_entrada_en_vigor": None}
contract_mod_schema = {"bid_id": None, "id_contrato": None, "id_modificacion": None, "plazo_modificacion": None,
                       "duracion": None, "importe_sin_impuestos": None, "importe_sin_impuestos_tras_mod": None}
publication_schema = {"bid_id": None, "tipo_anuncio": None, "medio_publicacion": None, "fecha_publicacion": None}
lot_schema = {"bid_id": None, "id_lote": None, "objeto_lote": None, "presupuesto_base_lote_sin_impuestos": None,
              "presupuesto_base_lote_total": None}
bid_cpv_schema = {"bid_id": None, "code": None, "code_description": None}
lot_cpv_schema = {"bid_id": None, "id_lote": None, "code": None, "code_description": None}
awarding_condition_schema = {"bid_id": None, "criterio_adjudicacion": None, "ponderacion_adjudicacion": None}
admission_condition_schema = {"bid_id": None, "condicion": None}
contract_extensions_schema = {"bid_id": None, "opcion": None, "periodo_validez": None}
required_guarantee_schema = {"bid_id": None, "tipo_garantia": None, "importe_garantia": None,
                             "porcentaje_garantia": None}
evaluation_criterion_schema = {"bid_id": None, "codigo_criterio": None, "descripcion_criterio": None,
                               "tipo_criterio": None}
required_business_classification_schema = {"bid_id": None, "codigo_clasificacion_empresarial": None,
                                           "clasificacion_empresarial": None}


def get_next_link(root):
    root = clean_atom_elements(root)
    for link in root.iterfind('link'):
        if link.attrib['rel'] == 'next':
            next_link = link.attrib['href']
            break
    return next_link, root


def parse_atom_feed(root, db_conn, bid_info_db, crawled_urls):
    bids_to_database = list()
    orgs_to_database = list()
    mods_to_database = list()
    winners_to_database = list()
    awarding_conditions_to_database = list()
    bid_cpvs_to_database = list()
    contract_extensions_to_database = list()
    docs_to_database = list()
    lot_cpvs_to_database = list()
    lots_to_database = list()
    events_to_database = list()
    guarantees_to_database = list()
    bussiness_class_to_database = list()
    admission_conditions_to_database = list()
    ev_criteria_to_database = list()
    publications_to_database = list()
    # CLEAN ELEMENTS
    for link in root.iterfind('link'):
        if link.attrib['rel'] == 'self':
            this_link = link.attrib['href']
            print(f'Processing atom file {this_link}. '
                  f'To store entries {len(root.findall("entry")) + len(root.findall("deleted-entry"))}')
            db_logger.debug(f'Processing atom file {this_link}. '
                            f'To store entries {len(root.findall("entry")) + len(root.findall("deleted-entry"))}')
            break
    # Get information for deleted entries
    for deleted_entry in root.iterfind('deleted-entry'):
        bid_metadata = bid_schema.copy()
        bid_uri = deleted_entry.attrib['ref']
        db_logger.debug(f'Processing bid {bid_uri}')
        deletion_date = deleted_entry.attrib['when']
        deletion_date, offset = parse_rfc3339_time(deletion_date)
        bid_metadata['bid_uri'] = bid_uri
        bid_metadata['deleted_at'] = deletion_date
        bid_metadata['deleted_at_offset'] = offset
        if not deleted_bid(bid_uri, bid_metadata, bid_info_db):
            bids_to_database.append(bid_metadata)
    for entry in root.iterfind('entry'):
        bid_metadata = bid_schema.copy()
        # Get mandatory info for bid
        bid_uri = entry.find('id').text  # Unique ID
        last_updated = entry.find('updated').text
        last_updated, offset = parse_rfc3339_time(last_updated)
        if not new_bid(bid_uri, bid_metadata, bid_info_db):
            bid_index = bid_info_db['bid_uri'].index(bid_uri)
            stored_last_updated = bid_info_db['last_updated'][bid_index]
            stored_offset = bid_info_db['last_updated_offset'][bid_index]
            if not more_recent_bid(bid_uri, last_updated, offset, stored_last_updated, stored_offset):
                continue
        bid_metadata['bid_uri'] = bid_uri
        bid_metadata['title'] = entry.find('title').text
        bid_metadata['link'] = entry.find('link').attrib['href']
        bid_metadata['last_updated'] = last_updated
        bid_metadata['last_updated_offset'] = offset

        # Process status information
        status = entry.find('ContractFolderStatus')
        # ESTADO (1)
        bid_status = status.find('ContractFolderStatusCode')
        code = bid_status.text
        url_bid_status = bid_status.attrib['listURI']
        if url_bid_status:
            status_dict = get_code_info(url_bid_status, crawled_urls)
            bid_metadata['bid_status'] = status_dict.get(code, code)
        # ID EXPEDIENTE (1)
        id_expediente = status.find('ContractFolderID')
        if id_expediente is not None:
            bid_metadata['id_expediente'] = status.find('ContractFolderID').text
        # PROCUREMENT PROJECT
        procurement_project = status.find('ProcurementProject')
        if procurement_project is not None:
            ## BUGDET AMOUNT
            budget_amount = procurement_project.find('BudgetAmount')
            if budget_amount is not None:
                ### VALOR ESTIMADO
                estimated_amount = budget_amount.find('EstimatedOverallContractAmount')
                if estimated_amount is not None:
                    bid_metadata['valor_estimado'] = float(estimated_amount.text)
                ### PRESUPUESTO SIN IMPUESTOS
                amount_wo_tax = budget_amount.find('TaxExclusiveAmount')
                if amount_wo_tax is not None:
                    bid_metadata['presupuesto_base_sin_impuestos'] = float(amount_wo_tax.text)
                ### PRESUPUESTO CON IMPUESTOS
                amount_w_tax = budget_amount.find('TotalAmount')
                if amount_w_tax is not None:
                    bid_metadata['presupuesto_base_total'] = float(amount_w_tax.text)
            ## DURACION (1)
            planned_period = procurement_project.find('PlannedPeriod')
            if planned_period is not None:
                ## 2 formas de expresar duracion
                duration = planned_period.find('DurationMeasure')
                if duration is not None:
                    bid_metadata['duracion'] = f'{duration.text} {duration.attrib["unitCode"]}'
                start_date = planned_period.find('StartDate')
                if start_date is not None:
                    bid_metadata['fecha_inicio'] = start_date.text.replace('Z', '')
                end_date = planned_period.find('EndDate')
                if end_date is not None:
                    bid_metadata['fecha_fin'] = end_date.text.replace('Z', '')
            ## CODIGO CPV (0-N)
            for code in procurement_project.iterfind('RequiredCommodityClassification'):
                cpv_metadata = bid_cpv_schema.copy()
                cpv_metadata['bid_id'] = bid_metadata['bid_uri']
                code_element = code.find('ItemClassificationCode')
                cpv_code = code_element.text
                cpv_metadata['code'] = cpv_code
                url_cpv_code = code_element.attrib['listURI']
                if url_cpv_code:
                    code_dict = get_code_info(url_cpv_code, crawled_urls)
                    cpv_metadata['code_description'] = code_dict.get(cpv_code, '')
                bid_cpvs_to_database.append(cpv_metadata)
            ## TIPO DE CONTRATO (0-1)
            type_element = procurement_project.find('TypeCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                if url_bid_type:
                    type_dict = get_code_info(url_bid_type, crawled_urls)
                    bid_metadata['tipo_contrato'] = type_dict.get(type, type)
            ## SUBTIPO DE CONTRATO (0-1)
            subtype_element = procurement_project.find('SubTypeCode')
            if subtype_element is not None:
                code = subtype_element.text
                url_code = subtype_element.attrib['listURI']
                if url_code:
                    code_dict = get_code_info(url_code, crawled_urls)
                    bid_metadata['subtipo_contrato'] = code_dict.get(code, '')
            ## EXTENSION DEL CONTRATO
            for contract_extension in procurement_project.iterfind('ContractExtension'):
                ext_metadata = contract_extensions_schema.copy()
                ext_metadata['bid_id'] = bid_metadata['bid_uri']
                option = contract_extension.find('OptionsDescription')
                if option is not None:
                    ext_metadata['opcion'] = option.text
                validity_period = contract_extension.find('OptionValidityPeriod/Description')
                if validity_period is not None:
                    ext_metadata['periodo_validez'] = validity_period.text
                contract_extensions_to_database.append(ext_metadata)
            ## LUGAR DE EJECUCION
            location = procurement_project.find('RealizedLocation')
            if location is not None:
                county_element = location.find('CountrySubentityCode')
                if county_element is not None:
                    county = county_element.text
                    url_bid_county = county_element.attrib['listURI']
                    if url_bid_county:
                        county_dict = get_code_info(url_bid_county, crawled_urls)
                        bid_metadata['comunidad_ejecucion'] = county_dict.get(county, county)
                address = location.find('Address')
                if address is not None:
                    city = address.find('CityName')
                    if city is not None:
                        bid_metadata['ciudad_ejecucion'] = city.text
                    cp = address.find('PostalZone')
                    if cp is not None:
                        bid_metadata['zona_postal_lugar_ejecucion'] = cp.text
                    country = address.find('Country/IdentificationCode')
                    if country is not None:
                        code = country.text
                        url_codes = country.attrib['listURI']
                        if url_codes:
                            code_dict = get_code_info(url_codes, crawled_urls)
                            bid_metadata['pais_ejecucion'] = code_dict.get(code, code)
        # DOCUMENTOS: Van en otra base de datos (1 licitación - N documentos)
        ## PLIEGO ADMINISTRATIVO (0-1)

        doc_element = status.find('LegalDocumentReference')
        if doc_element is not None:
            doc_metadata = doc_schema.copy()
            doc_metadata['bid_id'] = bid_metadata['bid_uri']
            doc_metadata['doc_id'] = doc_element.find('ID').text
            doc_metadata['doc_url'] = doc_element.find('Attachment/ExternalReference/URI').text
            doc_hash = doc_element.find('Attachment/ExternalReference/DocumentHash')
            if doc_hash is not None:
                doc_metadata['doc_hash'] = doc_hash.text
            doc_metadata['doc_type'] = 'administrativo'
            docs_to_database.append(doc_metadata)
        ## PLIEGO TECNICO (0-1)
        doc_element = status.find('TechnicalDocumentReference')
        if doc_element is not None:
            doc_metadata = doc_schema.copy()
            doc_metadata['bid_id'] = bid_metadata['bid_uri']
            doc_metadata['doc_id'] = doc_element.find('ID').text
            doc_metadata['doc_url'] = doc_element.find('Attachment/ExternalReference/URI').text
            if doc_element.find('Attachment/ExternalReference/DocumentHash') is not None:
                doc_metadata['doc_hash'] = doc_element.find('Attachment/ExternalReference/DocumentHash').text
            doc_metadata['doc_type'] = 'tecnico'
            docs_to_database.append(doc_metadata)
        ## OTROS DOCUMENTOS (0-N)
        for doc_element in status.iterfind('AdditionalDocumentReference'):
            doc_metadata = doc_schema.copy()
            doc_metadata['bid_id'] = bid_metadata['bid_uri']
            doc_metadata['doc_id'] = doc_element.find('ID').text
            doc_metadata['doc_url'] = doc_element.find('Attachment/ExternalReference/URI').text
            if doc_element.find('Attachment/ExternalReference/DocumentHash') is not None:
                doc_metadata['doc_hash'] = doc_element.find('Attachment/ExternalReference/DocumentHash').text
            doc_metadata['doc_type'] = 'otro'
            docs_to_database.append(doc_metadata)

        # LOTES (1)
        for lot_element in status.iterfind('ProcurementProjectLot'):
            lot_metadata = lot_schema.copy()
            lot_metadata['bid_id'] = bid_metadata['bid_uri']
            lot_metadata['id_lote'] = lot_element.find('ID').text
            ## OBJETO DEL LOTE (1)
            lote_object_element = lot_element.find('ProcurementProject')
            if lote_object_element is not None:
                lot_metadata['objeto_lote'] = lote_object_element.find('Name').text
                ### VALOR ESTIMADO E IMPORTE DE LOTE (0 o 1)
                budget = lote_object_element.find('BudgetAmount')
                if budget is not None:
                    amount_wo_tax = budget.find('TaxExclusiveAmount')
                    if amount_wo_tax is not None:
                        lot_metadata['presupuesto_base_lote_sin_impuestos'] = float(amount_wo_tax.text)
                    amount_w_tax = budget.find('TotalAmount')
                    if amount_w_tax is not None:
                        lot_metadata['presupuesto_base_lote_total'] = float(amount_w_tax.text)
            ## CODIGOS CPV DE LOTE

            for code in lote_object_element.iterfind('RequiredCommodityClassification'):
                cpv_metadata = lot_cpv_schema.copy()
                cpv_metadata['bid_id'] = bid_metadata['bid_uri']
                code_element = code.find('ItemClassificationCode')
                cpv_code = code_element.text
                cpv_metadata['code'] = cpv_code
                url_cpv_code = code_element.attrib['listURI']
                if url_cpv_code:
                    code_dict = get_code_info(url_cpv_code, crawled_urls)
                    cpv_metadata['code_description'] = code_dict.get(cpv_code, '')
                lot_cpvs_to_database.append(cpv_metadata)
            lots_to_database.append(lot_metadata)

        # TENDERING PROCESS
        tendering_process = status.find('TenderingProcess')
        if tendering_process is not None:
            ## TIPO DE PROCEDIMIENTO
            type_element = tendering_process.find('ProcedureCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                if url_bid_type:
                    type_dict = get_code_info(url_bid_type, crawled_urls)
                    bid_metadata['tipo_procedimiento'] = type_dict.get(type, type)
            ## SISTEMA DE CONTRATACION
            type_element = tendering_process.find('ContractingSystemCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                if url_bid_type:
                    type_dict = get_code_info(url_bid_type, crawled_urls)
                    bid_metadata['sistema_contratacion'] = type_dict.get(type, type)
            ## TIPO DE TRAMITACION
            type_element = tendering_process.find('UrgencyCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                if url_bid_type:
                    type_dict = get_code_info(url_bid_type, crawled_urls)
                    bid_metadata['tipo_tramitacion'] = type_dict.get(type, type)
            ## PRESENTACION DE LA OFERTA
            type_element = tendering_process.find('SubmissionMethodCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                if url_bid_type:
                    type_dict = get_code_info(url_bid_type, crawled_urls)
                    bid_metadata['presentacion_oferta'] = type_dict.get(type, type)
            ## PLAZO DE PLIEGOS
            plazo_pliego = tendering_process.find('DocumentAvailabilityPeriod')
            if plazo_pliego is not None:
                end_date = plazo_pliego.find('EndDate')
                end_time = plazo_pliego.find('EndTime')
                if end_date is not None and end_time is not None:
                    bid_metadata['plazo_pliegos'] = f"{end_date.text} {end_time.text}"
            ## PLAZO DE PRESENTACION DE OFERTAS
            plazo_presentacion = tendering_process.find('TenderSubmissionDeadlinePeriod')
            if plazo_presentacion is not None:
                description = plazo_presentacion.find('Description')
                if description is not None:
                    bid_metadata['deadline_descripcion'] = description.text
                end_date = plazo_presentacion.find('EndDate')
                end_time = plazo_presentacion.find('EndTime')
                if end_date is not None and end_time is not None:
                    bid_metadata['plazo_presentacion'] = f"{end_date.text.replace('Z', '')} {end_time.text}"
            ## EVENTOS
            for evento_element in tendering_process.iterfind('OpenTenderEvent'):
                event_metadata = event_schema.copy()
                event_metadata['bid_id'] = bid_metadata['bid_uri']
                event_type = evento_element.find('TypeCode')
                if event_type is not None:
                    type = event_type.text
                    url_event_type = event_type.attrib['listURI']
                    if url_event_type:
                        type_dict = get_code_info(url_event_type, crawled_urls)
                        event_metadata['tipo'] = type_dict.get(type, type)
                event_id = evento_element.find('IdentificationID')
                if event_id is not None:
                    event_metadata['id_evento'] = event_id.text
                event_date = evento_element.find('OccurrenceDate')
                event_time = evento_element.find('OccurrenceTime')
                if event_date is not None and event_time is not None:
                    event_metadata['fecha'] = f"{end_date.text} {end_time.text}"
                event_description = evento_element.find('Description')
                if event_description is not None:
                    event_metadata['descripcion'] = event_description.text
                event_place = evento_element.find('OccurrenceLocation')
                if event_place is not None:
                    place_element = event_place.find('Description')
                    if place_element is not None:
                        event_metadata['lugar'] = place_element.text
                    address_element = event_place.find('Address')
                    if address_element is not None:
                        address = address_element['AddressLine']
                        if address is not None:
                            event_metadata['direccion'] = address.text
                        cp = address_element['PostalZone']
                        if cp is not None:
                            event_metadata['cp'] = cp.text
                        city = address_element['CityName']
                        if city is not None:
                            event_metadata['ciudad'] = city.text
                        country = address_element['Country/IdentificationCode']
                        if country is not None:
                            country_code = country.text
                            url_country_code = country.attrib['listURI']
                            if url_country_code:
                                code_dict = get_code_info(url_country_code, crawled_urls)
                                event_metadata['pais'] = code_dict.get(country_code, country_code)
                events_to_database.append(event_metadata)

            ## LIMITACION NUMERO LICITADORES
            limitation_element = tendering_process.find('EconomicOperatorShortList')
            if limitation_element is not None:
                limitation_description = limitation_element.find('LimitationDescription')
                if limitation_description is not None:
                    bid_metadata['criterio_limitacion_numero_candidatos'] = limitation_description.text.upper()
                expected_cuantity = limitation_element.find('ExpectedQuantity')
                if expected_cuantity is not None:
                    bid_metadata['candidatos_esperados'] = expected_cuantity.text
                maximum_cuantity = limitation_element.find('MaximumQuantity')
                if maximum_cuantity is not None:
                    bid_metadata['candidatos_maximos'] = maximum_cuantity.text
                minimum_cuantity = limitation_element.find('MinimumQuantity')
                if minimum_cuantity is not None:
                    bid_metadata['candidatos_minimos'] = minimum_cuantity.text
            ## JUSTIFICACION PROCESO EXTRAORDINARIO
            justification = tendering_process.find('ProcessJustification')
            if justification is not None:
                reason_code = justification.find('ReasonCode')
                if reason_code is not None:
                    code = reason_code.text
                    url_code = reason_code.attrib['listURI']
                    if url_code:
                        code_dict = get_code_info(url_code, crawled_urls)
                        bid_metadata['codigo_justificacion_proceso_extraordinario'] = code_dict.get(code, code)
                description = justification.find('Description')
                if description is not None:
                    bid_metadata['descripcion_justificacion_proceso_extraordinario'] = description.text
        # TENDERING TERMS
        tendering_terms = status.find('TenderingTerms')
        if tendering_terms is not None:
            ## IDIOMA
            language = tendering_terms.find('Language/ID')
            if language is not None:
                bid_metadata['idioma'] = language.text.upper()
            ## PROVEEDOR DE PLIEGOS
            provider_element = tendering_terms.find('DocumentProviderParty')
            if provider_element is not None:
                org_metadata = org_schema.copy()
                org_metadata['bid_id'] = bid_metadata['bid_uri']
                provider_name = provider_element.find('PartyName/Name')
                org_metadata['nombre'] = unidecode(provider_name).strip()
                bid_metadata['proveedor_pliegos'] = provider_name
                provider_website = provider_element.find('WebsiteURI')
                if provider_website is not None:
                    org_metadata['uri'] = provider_website
                provider_addr_info = provider_element.find('PostalAddress')
                if provider_addr_info is not None:
                    provider_addr = provider_addr_info.find('AddressLine')
                    provider_cp = provider_addr_info.find('PostalZone')
                    provider_city = provider_addr_info.find('CityName')
                    provider_country = provider_addr_info.find('Country/IdentificationCode')
                    if provider_addr is not None:
                        org_metadata['direccion'] = provider_addr.text
                    if provider_cp is not None:
                        org_metadata['cp'] = provider_cp.text
                    if provider_city is not None:
                        org_metadata['ciudad'] = provider_city.text
                    if provider_country is not None:
                        country_code = provider_country.text
                        url_country_code = provider_country.attrib['listURI']
                        if url_country_code:
                            code_dict = get_code_info(url_country_code, crawled_urls)
                            org_metadata['pais'] = code_dict.get(country_code, country_code)
                provider_contact_info = provider_element.find('Contact')
                if provider_contact_info is not None:
                    provider_name = provider_contact_info.find('Name')
                    provider_telephone = provider_contact_info.find('Telephone')
                    provider_fax = provider_contact_info.find('Telefax')
                    provider_email = provider_contact_info.find('ElectronicMail')
                    if provider_name is not None:
                        org_metadata['nombre_contacto'] = provider_name.text
                    if provider_telephone is not None:
                        org_metadata['telefono_contacto'] = provider_telephone.text
                    if provider_fax is not None:
                        org_metadata['fax_contacto'] = provider_fax.text
                    if provider_email is not None:
                        org_metadata['email_contacto'] = provider_email.text
                orgs_to_database.append(org_metadata)
            ## LUGAR RECEPCION OFERTAS
            receiving_element = tendering_terms.find('TenderRecipientParty')
            if receiving_element is not None:
                org_metadata = org_schema.copy()
                org_metadata['bid_id'] = bid_metadata['bid_uri']
                receiving_name = receiving_element.find('PartyName/Name')
                org_metadata['nombre'] = unidecode(receiving_name).strip()
                bid_metadata['receptor_ofertas'] = receiving_name
                receiving_website = receiving_element.find('WebsiteURI')
                if receiving_website is not None:
                    org_metadata['uri'] = receiving_website
                receiving_addr_info = receiving_element.find('PostalAddress')
                if receiving_addr_info is not None:
                    receiving_addr = receiving_addr_info.find('AddressLine')
                    receiving_cp = receiving_addr_info.find('PostalZone')
                    receiving_city = receiving_addr_info.find('CityName')
                    receiving_country = receiving_addr_info.find('Country/IdentificationCode')
                    if receiving_addr is not None:
                        org_metadata['direccion'] = receiving_addr.text
                    if receiving_cp is not None:
                        org_metadata['cp'] = receiving_cp.text
                    if receiving_city is not None:
                        org_metadata['ciudad'] = receiving_city.text
                    if receiving_country is not None:
                        country_code = receiving_country.text
                        url_country_code = receiving_country.attrib['listURI']
                        if url_country_code:
                            code_dict = get_code_info(url_country_code, crawled_urls)
                            org_metadata['pais'] = code_dict.get(country_code, country_code)
                receiving_contact_info = receiving_element.find('Contact')
                if receiving_contact_info is not None:
                    receiving_name = receiving_contact_info.find('Name')
                    receiving_telephone = receiving_contact_info.find('Telephone')
                    receiving_fax = receiving_contact_info.find('Telefax')
                    receiving_email = receiving_contact_info.find('ElectronicMail')
                    if receiving_name is not None:
                        org_metadata['nombre_contacto'] = receiving_name.text
                    if receiving_telephone is not None:
                        org_metadata['telefono_contacto'] = receiving_telephone.text
                    if receiving_fax is not None:
                        org_metadata['fax_contacto'] = receiving_fax.text
                    if receiving_email is not None:
                        org_metadata['email_contacto'] = receiving_email.text
                orgs_to_database.append(org_metadata)

            ## PROVEEDOR DE INFORMACION ADICIONAL
            provider_element = tendering_terms.find('AdditionalInformationParty')
            if provider_element is not None:
                org_metadata = org_schema.copy()
                org_metadata['bid_id'] = bid_metadata['bid_uri']
                provider_name = provider_element.find('PartyName/Name')
                org_metadata['nombre'] = unidecode(provider_name).strip()
                bid_metadata['proveedor_informacion_adicional'] = provider_name
                provider_website = provider_element.find('WebsiteURI')
                if provider_website is not None:
                    org_metadata['uri'] = provider_website
                provider_addr_info = provider_element.find('PostalAddress')
                if provider_addr_info is not None:
                    provider_addr = provider_addr_info.find('AddressLine')
                    provider_cp = provider_addr_info.find('PostalZone')
                    provider_city = provider_addr_info.find('CityName')
                    provider_country = provider_addr_info.find('Country/IdentificationCode')
                    if provider_addr is not None:
                        org_metadata['direccion'] = provider_addr.text
                    if provider_cp is not None:
                        org_metadata['cp'] = provider_cp.text
                    if provider_city is not None:
                        org_metadata['ciudad'] = provider_city.text
                    if provider_country is not None:
                        country_code = provider_country.text
                        url_country_code = provider_country.attrib['listURI']
                        if url_country_code:
                            code_dict = get_code_info(url_country_code, crawled_urls)
                            org_metadata['pais'] = code_dict.get(country_code, country_code)
                provider_contact_info = provider_element.find('Contact')
                if provider_contact_info is not None:
                    provider_name = provider_contact_info.find('Name')
                    provider_telephone = provider_contact_info.find('Telephone')
                    provider_fax = provider_contact_info.find('Telefax')
                    provider_email = provider_contact_info.find('ElectronicMail')
                    if provider_name is not None:
                        org_metadata['nombre_contacto'] = provider_name.text
                    if provider_telephone is not None:
                        org_metadata['telefono_contacto'] = provider_telephone.text
                    if provider_fax is not None:
                        org_metadata['fax_contacto'] = provider_fax.text
                    if provider_email is not None:
                        org_metadata['email_contacto'] = provider_email.text
                orgs_to_database.append(org_metadata)

            ## APPEAL TERMS
            appeal_terms = tendering_terms.find('AppealTerms')
            if appeal_terms is not None:
                ### INFORMACION SOBRE RECURSOS
                resource_info_element = appeal_terms.find('AppealInformationParty')
                if resource_info_element is not None:
                    org_metadata = org_schema.copy()
                    org_metadata['bid_id'] = bid_metadata['bid_uri']
                    resource_name = resource_info_element.find('PartyName/Name')
                    org_metadata['nombre'] = unidecode(resource_name).strip()
                    bid_metadata['info_recursos'] = resource_name
                    resource_website = resource_info_element.find('WebsiteURI')
                    if resource_website is not None:
                        org_metadata['uri'] = resource_website
                    resource_addr_info = resource_info_element.find('PostalAddress')
                    if resource_addr_info is not None:
                        resource_addr = resource_addr_info.find('AddressLine')
                        resource_cp = resource_addr_info.find('PostalZone')
                        resource_city = resource_addr_info.find('CityName')
                        resource_country = resource_addr_info.find('Country/IdentificationCode')
                        if resource_addr is not None:
                            org_metadata['direccion'] = resource_addr.text
                        if resource_cp is not None:
                            org_metadata['cp'] = resource_cp.text
                        if resource_city is not None:
                            org_metadata['ciudad'] = resource_city.text
                        if resource_country is not None:
                            country_code = resource_country.text
                            url_country_code = resource_country.attrib['listURI']
                            if url_country_code:
                                code_dict = get_code_info(url_country_code, crawled_urls)
                                org_metadata['pais'] = code_dict.get(country_code, country_code)
                    resource_contact_info = resource_info_element.find('Contact')
                    if resource_contact_info is not None:
                        resource_name = resource_contact_info.find('Name')
                        resource_telephone = resource_contact_info.find('Telephone')
                        resource_fax = resource_contact_info.find('Telefax')
                        resource_email = resource_contact_info.find('ElectronicMail')
                        if resource_name is not None:
                            org_metadata['nombre_contacto'] = resource_name.text
                        if resource_telephone is not None:
                            org_metadata['telefono_contacto'] = resource_telephone.text
                        if resource_fax is not None:
                            org_metadata['fax_contacto'] = resource_fax.text
                        if resource_email is not None:
                            org_metadata['email_contacto'] = resource_email.text
                    orgs_to_database.append(org_metadata)

                ### LUGAR DE PRESENTACION DE RECURSOS
                resource_presentation_element = appeal_terms.find('AppealReceiverParty')
                if resource_presentation_element is not None:
                    org_metadata = org_schema.copy()
                    org_metadata['bid_id'] = bid_metadata['bid_uri']
                    resource_name = resource_presentation_element.find('PartyName/Name')
                    org_metadata['nombre'] = unidecode(resource_name).strip()
                    bid_metadata['receptor_recursos'] = resource_name
                    resource_website = resource_presentation_element.find('WebsiteURI')
                    if resource_website is not None:
                        org_metadata['uri'] = resource_website
                    resource_addr_info = resource_presentation_element.find('PostalAddress')
                    if resource_addr_info is not None:
                        resource_addr = resource_addr_info.find('AddressLine')
                        resource_cp = resource_addr_info.find('PostalZone')
                        resource_city = resource_addr_info.find('CityName')
                        resource_country = resource_addr_info.find('Country/IdentificationCode')
                        if resource_addr is not None:
                            org_metadata['direccion'] = resource_addr.text
                        if resource_cp is not None:
                            org_metadata['cp'] = resource_cp.text
                        if resource_city is not None:
                            org_metadata['ciudad'] = resource_city.text
                        if resource_country is not None:
                            country_code = resource_country.text
                            url_country_code = resource_country.attrib['listURI']
                            if url_country_code:
                                code_dict = get_code_info(url_country_code, crawled_urls)
                                org_metadata['pais'] = code_dict.get(country_code, country_code)
                    resource_contact_info = resource_presentation_element.find('Contact')
                    if resource_contact_info is not None:
                        resource_name = resource_contact_info.find('Name')
                        resource_telephone = resource_contact_info.find('Telephone')
                        resource_fax = resource_contact_info.find('Telefax')
                        resource_email = resource_contact_info.find('ElectronicMail')
                        if resource_name is not None:
                            org_metadata['nombre_contacto'] = resource_name.text
                        if resource_telephone is not None:
                            org_metadata['telefono_contacto'] = resource_telephone.text
                        if resource_fax is not None:
                            org_metadata['fax_contacto'] = resource_fax.text
                        if resource_email is not None:
                            org_metadata['email_contacto'] = resource_email.text
                    orgs_to_database.append(org_metadata)

                ### FECHA LIMITE DE PRESENTACION DE RECURSOS
                presentation_period_element = appeal_terms.find('PresentationPeriod')
                if presentation_period_element is not None:
                    end_date = presentation_period_element.find('EndDate')
                    end_time = presentation_period_element.find('EndTime')
                    if end_date is not None and end_time is not None:
                        bid_metadata['plazo_presentacion_recursos'] = f"{end_date.text} {end_time.text}"
                ### ARBITRAJE
                mediation_element = appeal_terms.find('AppealReceiverParty')
                if mediation_element is not None:
                    org_metadata = org_schema.copy()
                    org_metadata['bid_id'] = bid_metadata['bid_uri']
                    mediation_name = mediation_element.find('PartyName/Name')
                    org_metadata['nombre'] = unidecode(mediation_name).strip()
                    bid_metadata['organo_mediador'] = mediation_name
                    mediation_website = mediation_element.find('WebsiteURI')
                    if mediation_website is not None:
                        org_metadata['uri'] = mediation_website
                    mediation_addr_info = mediation_element.find('PostalAddress')
                    if mediation_addr_info is not None:
                        mediation_addr = mediation_addr_info.find('AddressLine')
                        mediation_cp = mediation_addr_info.find('PostalZone')
                        mediation_city = mediation_addr_info.find('CityName')
                        mediation_country = mediation_addr_info.find('Country/IdentificationCode')
                        if mediation_addr is not None:
                            org_metadata['direccion'] = mediation_addr.text
                        if mediation_cp is not None:
                            org_metadata['cp'] = mediation_cp.text
                        if mediation_city is not None:
                            org_metadata['ciudad'] = mediation_city.text
                        if mediation_country is not None:
                            country_code = mediation_country.text
                            url_country_code = mediation_country.attrib['listURI']
                            if url_country_code:
                                code_dict = get_code_info(url_country_code, crawled_urls)
                                org_metadata['pais'] = code_dict.get(country_code, country_code)
                    mediation_contact_info = mediation_element.find('Contact')
                    if mediation_contact_info is not None:
                        mediation_name = mediation_contact_info.find('Name')
                        mediation_telephone = mediation_contact_info.find('Telephone')
                        mediation_fax = mediation_contact_info.find('Telefax')
                        mediation_email = mediation_contact_info.find('ElectronicMail')
                        if mediation_name is not None:
                            org_metadata['nombre_contacto'] = mediation_name.text
                        if mediation_telephone is not None:
                            org_metadata['telefono_contacto'] = mediation_telephone.text
                        if mediation_fax is not None:
                            org_metadata['fax_contacto'] = mediation_fax.text
                        if mediation_email is not None:
                            org_metadata['email_contacto'] = mediation_email.text
                    orgs_to_database.append(org_metadata)

            ## CURRICULUM REQUERIDO
            required_curriculum = tendering_terms.find('RequiredCurriculaIndicator')
            if required_curriculum is not None:
                bid_metadata['curriculum_requerido'] = (required_curriculum.text.title() == 'True')
            ## ADMISION DE VARIANTES
            admision_variantes = tendering_terms.find('VariantConstraintIndicator')
            if admision_variantes is not None:
                bid_metadata['admision_variantes'] = (admision_variantes.text.title() == 'True')
            ## FORMULA DE REVISION DE PRECIOS
            price_revision = tendering_terms.find('PriceRevisionFormulaDescription')
            if price_revision is not None:
                bid_metadata['formula_revision_precio'] = price_revision.text
            ## PROGRAMA DE FINANCIACION
            funding_program_code = tendering_terms.find('FundingProgramCode')
            funding_program = tendering_terms.find('FundingProgram')
            funding_program_text = str()
            if funding_program_code is not None:
                code = funding_program_code.text
                url_code = funding_program_code.attrib['listURI']
                if url_code:
                    code_dict = get_code_info(url_code, crawled_urls)
                    funding_program_text = code_dict.get(code, code)
            if funding_program is not None:
                funding_program_text += f' {funding_program.text}'
            if funding_program_text:
                bid_metadata['programa_financiacion'] = funding_program_text.strip()
            ## GARANTIAS REQUERIDAS
            for guarantee_element in tendering_terms.iterfind('RequiredFinancialGuarantee'):
                guarantee_metadata = required_guarantee_schema.copy()
                guarantee_metadata['bid_id'] = bid_metadata['bid_uri']
                guarantee_type = guarantee_element.find('GuaranteeTypeCode')
                if guarantee_type is not None:
                    code = guarantee_type.text
                    url_code = guarantee_type.attrib['listURI']
                    if url_code:
                        code_dict = get_code_info(url_code, crawled_urls)
                        guarantee_metadata['tipo_garantia'] = code_dict.get(code, code)
                guarantee_amount = guarantee_element.find('LiabilityAmount')
                if guarantee_amount is not None:
                    guarantee_metadata['importe_garantia'] = guarantee_amount.text
                guarantee_rate = guarantee_element.find('AmountRate')
                if guarantee_rate is not None:
                    guarantee_metadata['porcentaje_garantia'] = guarantee_rate.text
                guarantees_to_database.append(guarantee_metadata)

            ## REQUISITOS DE PARTICIPACION
            requisites_element = tendering_terms.find('TendererQualificationRequest')
            if requisites_element is not None:
                ### TITULO HABILITANTE
                personal_situation = requisites_element.find('PersonalSituation')
                if personal_situation is not None:
                    bid_metadata['titulo_habilitante'] = personal_situation.text
                ### DESCRIPCION
                description = tendering_terms.find('Description')
                if description is not None:
                    bid_metadata['descripcion_requisitos_participacion'] = description.text
                ### CLASIFICACION EMPRESARIAL
                for required_bussiness_class in requisites_element.iterfind(
                        'RequiredBusinessClassificationScheme/ClassificationCategory'):
                    class_metadata = required_business_classification_schema.copy()
                    class_metadata['bid_id'] = bid_metadata['bid_uri']
                    description = required_bussiness_class.find('Description')
                    if description is not None:
                        class_metadata['clasificacion_empresarial'] = required_bussiness_class
                    code = required_bussiness_class.find('CodeValue')
                    if code is not None:
                        class_metadata['codigo_clasificacion_empresarial'] = code.text
                    bussiness_class_to_database.append(class_metadata)

                ### CONDICIONES DE ADMISION
                for condition in requisites_element.iterfind('SpecificTendererRequirement/RequirementTypeCode'):
                    cond_metadata = admission_condition_schema.copy()
                    cond_metadata['bid_id'] = bid_metadata['bid_uri']
                    code = condition.text
                    url_code = condition.attrib['listURI']
                    if url_code:
                        code_dict = get_code_info(url_code, crawled_urls)
                        cond_metadata['condicion'] = code_dict.get(code, code)
                    admission_conditions_to_database.append(cond_metadata)

                ### CRITERIO DE EVALUACION TECNICA
                for tech_criteria in requisites_element.iterfind('TechnicalEvaluationCriteria'):
                    crit_metadata = evaluation_criterion_schema.copy()
                    crit_metadata['bid_id'] = bid_metadata['bid_uri']
                    crit_metadata['tipo_criterio'] = 'TECNICO'
                    criteria_code = tech_criteria.find('EvaluationCriteriaTypeCode')
                    if criteria_code is not None:
                        code = criteria_code.text
                        url_code = criteria_code.attrib['listURI']
                        if url_code:
                            code_dict = get_code_info(url_code, crawled_urls)
                            crit_metadata['codigo_criterio'] = code_dict.get(code, code)
                    criteria_description = tech_criteria.find('Description')
                    if criteria_description is not None:
                        crit_metadata['descripcion_criterio'] = criteria_description.text
                    ev_criteria_to_database.append(crit_metadata)
                ### CRITERIO DE EVALUACION ECONOMICO-FINANCIERA
                for finantial_criteria in requisites_element.iterfind('FinancialEvaluationCriteria'):
                    crit_metadata = evaluation_criterion_schema.copy()
                    crit_metadata['bid_id'] = bid_metadata['bid_uri']
                    crit_metadata['tipo_criterio'] = 'ECONOMICO-FINANCIERO'
                    criteria_code = finantial_criteria.find('EvaluationCriteriaTypeCode')
                    if criteria_code is not None:
                        code = criteria_code.text
                        url_code = criteria_code.attrib['listURI']
                        if url_code:
                            code_dict = get_code_info(url_code, crawled_urls)
                            crit_metadata['codigo_criterio'] = code_dict.get(code, code)
                    criteria_description = finantial_criteria.find('Description')
                    if criteria_description is not None:
                        crit_metadata['descripcion_criterio'] = criteria_description.text
                    ev_criteria_to_database.append(crit_metadata)

            ## SUBCONTRATACION PERMITIDA
            subcontract_terms = tendering_terms.find('AllowedSubcontractTerms')
            if subcontract_terms is not None:
                subcontract_rate = subcontract_terms.find('Rate')
                if subcontract_rate is not None:
                    bid_metadata['porcentaje_maximo_subcontratacion'] = subcontract_rate.text
                subcontract_description = subcontract_terms.find('Description')
                if subcontract_description is not None:
                    bid_metadata['objeto_subcontratacion'] = subcontract_description.text
            ## PREPARACION OFERTA
            tender_preparation = tendering_terms.find('TenderPreparation')
            if tender_preparation is not None:
                envelope_name = tender_preparation.find('TenderEnvelopeID')
                if envelope_name is not None:
                    bid_metadata['id_sobre'] = envelope_name.text
                envelope_type = tender_preparation.find('TenderEnvelopeTypeCode')
                if envelope_type is not None:
                    code = envelope_type.text
                    url_code = envelope_type.attrib['listURI']
                    if url_code:
                        code_dict = get_code_info(url_code, crawled_urls)
                        bid_metadata['tipo_documento_sobre'] = code_dict.get(code, code)
                envelope_description = tender_preparation.find('Description')
                if envelope_description is not None:
                    bid_metadata['descripcion_preparacion_oferta'] = envelope_description.text
            ## CONDICIONES ADJUDICACION
            for awarding_terms in tendering_terms.iterfind('AwardingTerms/AwardingCriteria'):
                condition_metadata = awarding_condition_schema.copy()
                condition_metadata['bid_id'] = bid_metadata['bid_uri']
                term_description = awarding_terms.find('Description')
                if term_description is not None:
                    condition_metadata['criterio_adjudicacion'] = term_description.text
                weight = awarding_terms.find('WeightNumeric')
                if weight is not None:
                    condition_metadata['ponderacion_adjudicacion'] = weight.text
                awarding_conditions_to_database.append(condition_metadata)

        # RESULTADO PROCEDIMIENTO
        for result in status.iterfind('TenderResult'):
            winner_metadata = winner_schema.copy()
            winner_metadata['bid_id'] = bid_metadata['bid_uri']
            code_element = result.find('ResultCode')
            if code_element is not None:
                code = code_element.text
                url_code = code_element.attrib['listURI']
                if url_code:
                    code_dict = get_code_info(url_code, crawled_urls)
                    ## ESTADO FINAL ADJUDIACION
                    winner_metadata['resultado'] = code_dict.get(code, code)
            ## ADJUDICATARIO
            winner = result.find('WinningParty')
            if winner is not None:
                org_metadata = org_schema.copy()
                org_metadata['bid_id'] = bid_metadata['bid_uri']
                winner_metadata['adjudicatario'] = unidecode(winner.find('PartyName/Name').text).strip()
                org_metadata['nombre'] = unidecode(winner_metadata['adjudicatario']).strip()
                winner_id = winner.find('PartyIdentification/ID')
                org_metadata['id'] = winner_id.text
                org_metadata['razon_social'] = winner_id.attrib['schemeName']
                orgs_to_database.append(org_metadata)
            awarded_tendered_project = result.find('AwardedTenderedProject')
            if awarded_tendered_project is not None:
                ### IMPORTE ADJUDICACION
                amount = awarded_tendered_project.find('LegalMonetaryTotal')
                if amount is not None:
                    amount_wo_tax = amount.find('TaxExclusiveAmount')
                    if amount_wo_tax is not None:
                        winner_metadata['importe_sin_impuestos'] = float(amount_wo_tax.text)
                    amount_w_tax = amount.find('PayableAmount')
                    if amount_w_tax is not None:
                        winner_metadata['importe_con_impuestos'] = float(amount_w_tax.text)
                ### ID LOTE
                lot_id = awarded_tendered_project.find('ProcurementProjectLotID')
                if lot_id is not None:
                    winner_metadata['id_lote'] = int(lot_id.text)
            ## NUMERO DE PARTICIPANTES
            num_licitadores = result.find('ReceivedTenderQuantity')
            if num_licitadores is not None:
                winner_metadata['numero_participantes'] = float(num_licitadores.text)
            ## MOTIVACION
            motivation = result.find('Description')
            if motivation is not None:
                winner_metadata['motivacion'] = motivation.text
            ## FECHA
            date = result.find('AwardDate')
            if date is not None:
                winner_metadata['fecha_adjudicacion'] = date.text.replace('Z', '')
            ## OFERTAS RECIBIDAS
            highest_offer = result.find('HigherTenderAmount')
            if highest_offer is not None:
                winner_metadata['maxima_oferta_recibida'] = float(highest_offer.text)
            lowest_offer = result.find('LowerTenderAmount')
            if lowest_offer is not None:
                winner_metadata['minima_oferta_recibida'] = float(lowest_offer.text)
            ## INFORMACION SOBRE EL CONTRATO
            contract_info = result.find('Contract')
            if contract_info is not None:
                ### ID CONTRATO
                contract_id = contract_info.find('ID')
                if contract_id is not None:
                    winner_metadata['id_contrato'] = contract_id.text
                ### FECHA FORMALIZACION
                issue_date = contract_info.find('IssueDate')
                if issue_date is not None:
                    winner_metadata['fecha_formalizacion'] = issue_date.text.replace('Z', '')
            ## FECHA DE ENTRAGA EN VIGOR DEL CONTRATO
            entrada_vigor = result.find('StartDate')
            if entrada_vigor is not None:
                winner_metadata['fecha_entrada_en_vigor'] = entrada_vigor.text
            ## CONDICIONES DE SUBCONTRATACION
            subcontract_terms = result.find('SubcontractTerms')
            if subcontract_terms is not None:
                rate = subcontract_terms.find('Rate')
                if rate is not None:
                    winner_metadata['porcentaje_subcontratacion'] = float(rate.text)
                subcontract_description = subcontract_terms.find('Description')
                if subcontract_description is not None:
                    winner_metadata['objeto_subcontratacion'] = subcontract_description.text
            winners_to_database.append(winner_metadata)
        # ORGANO DE CONTRATACION
        org_metadata = org_schema.copy()
        org_metadata['bid_id'] = bid_metadata['bid_uri']
        located_contracting_party = status.find('LocatedContractingParty')
        upper_levels = str()
        org_element = located_contracting_party.find('Party')
        lowest_level = org_element.find('PartyName/Name').text
        org_metadata['nombre'] = unidecode(lowest_level).strip()
        org_id = org_element.find('PartyIdentification/ID')
        if org_id is not None:
            org_metadata['id'] = org_id.text
            org_metadata['razon_social'] = org_id.attrib['schemeName']

        org_website = org_element.find('WebsiteURI')
        if org_website is not None:
            org_metadata['uri'] = org_website.text
        org_type_code = located_contracting_party.find('ContractingPartyTypeCode')
        if org_type_code is not None:
            code = org_type_code.text
            url_code = org_type_code.attrib['listURI']
            if url_code:
                code_dict = get_code_info(url_code, crawled_urls)
                org_metadata['tipo_organismo'] = code_dict.get(code, code)
        org_addr_info = org_element.find('PostalAddress')
        if org_addr_info is not None:
            org_addr = org_addr_info.find('AddressLine')
            org_cp = org_addr_info.find('PostalZone')
            org_city = org_addr_info.find('CityName')
            org_country = org_addr_info.find('Country/IdentificationCode')
            if org_addr is not None:
                org_metadata['direccion'] = org_addr.text
            if org_cp is not None:
                org_metadata['cp'] = org_cp.text
            if org_city is not None:
                org_metadata['ciudad'] = org_city.text
            if org_country is not None:
                country_code = org_country.text
                url_country_code = org_country.attrib['listURI']
                if url_country_code:
                    code_dict = get_code_info(url_country_code, crawled_urls)
                    org_metadata['pais'] = code_dict.get(country_code, country_code)
        org_contact_info = org_element.find('Contact')
        if org_contact_info is not None:
            org_name = org_contact_info.find('Name')
            org_telephone = org_contact_info.find('Telephone')
            org_fax = org_contact_info.find('Telefax')
            org_email = org_contact_info.find('ElectronicMail')
            if org_name is not None:
                org_metadata['nombre_contacto'] = org_name.text
            if org_telephone is not None:
                org_metadata['telefono_contacto'] = org_telephone.text
            if org_fax is not None:
                org_metadata['fax_contacto'] = org_fax.text
            if org_email is not None:
                org_metadata['email_contacto'] = org_email.text
        orgs_to_database.append(org_metadata)
        hiera_element = located_contracting_party.find('ParentLocatedParty')
        if hiera_element is not None:
            upper_levels = get_full_contractor_name(hiera_element)
        if upper_levels:
            bid_metadata['organo_de_contratacion'] = f'{upper_levels} > {lowest_level}'
        else:
            bid_metadata['organo_de_contratacion'] = lowest_level

        # MODIFICACIONES DE CONTRATO
        for mod in status.iterfind('ContractModification'):
            mod_metadata = contract_mod_schema.copy()
            mod_metadata['bid_id'] = bid_metadata['bid_uri']
            ## ID CONTRATO
            contract_id = mod.find('ContractID')
            if contract_id is not None:
                mod_metadata['id_contrato'] = contract_id.text
            ## ID MODIFICACION
            mod_id = mod.find('ID')
            if mod_id is not None:
                mod_metadata['id_modificacion'] = mod_id.text
            ## IMPORTE SIN IMPUESTOS
            amount_wo_tax = mod.find('ContractModificationLegalMonetaryTotal/TaxExclusiveAmount')
            if amount_wo_tax is not None:
                mod_metadata['importe_sin_impuestos'] = amount_wo_tax.text
            ## IMPORTE SIN IMPUESTOS DEL CONTRATO TRAS MODIFICACION
            amount_wo_tax = mod.find('FinalLegalMonetaryTotal/TaxExclusiveAmount')
            if amount_wo_tax is not None:
                mod_metadata['importe_sin_impuestos_tras_mod'] = amount_wo_tax.text
            ## PLAZO MODIFICACION
            duration = mod.find('ContractModificationDurationMeasure')
            if duration is not None:
                mod_metadata['plazo_modificacion'] = f'{duration.text} {duration.attrib["unitCode"]}'
            ## DURACION CONTRATO
            duration = mod.find('FinalDurationMeasure')
            if duration is not None:
                mod_metadata['duracion'] = f'{duration.text} {duration.attrib["unitCode"]}'
            mods_to_database.append(mod_metadata)

        # MEDIOS DE PUBLICACION
        for medium in status.iterfind('ValidNoticeInfo'):
            medium_metadata = publication_schema.copy()
            medium_metadata['bid_id'] = bid_metadata['bid_uri']
            add_type = medium.find('NoticeTypeCode')
            if add_type is not None:
                code = add_type.text
                url_code = add_type.attrib['listURI']
                if url_code:
                    code_dict = get_code_info(url_code, crawled_urls)
                    medium_metadata['tipo_anuncio'] = code_dict.get(code, code)
            ## Puede haber varios medios de publicacion para un mismo tipo de anuncio
            for pub_status in medium.iterfind('AdditionalPublicationStatus'):
                copy_pub_metadata = medium_metadata.copy()
                media_name = pub_status.find('PublicationMediaName')
                if media_name is not None:
                    copy_pub_metadata['medio_publicacion'] = media_name.text
                for pub_date in pub_status.iterfind('AdditionalPublicationDocumentReference/IssueDate'):
                    copy2_pub_metadata = copy_pub_metadata.copy()
                    copy2_pub_metadata['fecha_publicacion'] = pub_date.text
                    publications_to_database.append(copy2_pub_metadata)
        bids_to_database.append(bid_metadata)
    if mods_to_database:
        insert_or_update_records(db_conn, mods_to_database, 'contract_mods')
    if winners_to_database:
        insert_or_update_records(db_conn, winners_to_database, 'winners')
    if awarding_conditions_to_database:
        insert_or_update_records(db_conn, awarding_conditions_to_database, "awarding_conditions")
    if bid_cpvs_to_database:
        insert_or_update_records(db_conn, bid_cpvs_to_database, 'bid_cpv_codes')
    if contract_extensions_to_database:
        insert_or_update_records(db_conn, contract_extensions_to_database, 'contract_extensions')
    if docs_to_database:
        insert_or_update_records(db_conn, docs_to_database, 'docs')
    if lot_cpvs_to_database:
        insert_or_update_records(db_conn, lot_cpvs_to_database, 'lot_cpv_codes')
    if lots_to_database:
        insert_or_update_records(db_conn, lots_to_database, 'lots')
    if events_to_database:
        insert_or_update_records(db_conn, events_to_database, 'events')
    if guarantees_to_database:
        insert_or_update_records(db_conn, guarantees_to_database, 'required_guarantees')
    if bussiness_class_to_database:
        insert_or_update_records(db_conn, bussiness_class_to_database, 'required_business_classifications')
    if admission_conditions_to_database:
        insert_or_update_records(db_conn, admission_conditions_to_database, 'admission_conditions')
    if ev_criteria_to_database:
        insert_or_update_records(db_conn, ev_criteria_to_database, 'evaluation_criteria')
    if publications_to_database:
        insert_or_update_records(db_conn, publications_to_database, 'publications')
    if orgs_to_database:
        insert_or_update_records(db_conn, orgs_to_database, 'orgs')
    if bids_to_database:
        bids_to_database = clean_bid_list(bids_to_database)
        insert_or_update_records(db_conn, bids_to_database, 'bids')
        db_logger.debug('Reloading bid and organization information from database...')
        print('Updating bids')
        bid_info_db = update_bid_info_db(bids_to_database, bid_info_db)
    return bid_info_db, crawled_urls


def update_bid_info_db(inserted_bids, bid_info_db):
    for bid in inserted_bids:
        if bid['bid_uri'] in bid_info_db['bid_uri']:
            bid_index = bid_info_db['bid_uri'].index(bid['bid_uri'])
            if bid['last_updated'] is not None:
                bid_info_db['last_updated'][bid_index] = bid['last_updated']
                bid_info_db['last_updated_offset'][bid_index] = bid['last_updated_offset']
            if bid['deleted_at_offset'] is not None:
                bid_info_db['deleted_at_offset'][bid_index] = bid['deleted_at_offset']
        else:
            bid_info_db['bid_uri'].append(bid['bid_uri'])
            bid_info_db['last_updated'].append(bid['last_updated'])
            bid_info_db['last_updated_offset'].append(bid['last_updated_offset'])
            bid_info_db['deleted_at_offset'].append(bid['deleted_at_offset'])
    return bid_info_db


def clean_bid_list(bids_to_database):
    # db_logger.debug('Storing or updating bids in database...')
    # Order bid list by last update, keeping deleted bids first
    deleted_bids_to_db = [bid for bid in bids_to_database if bid['last_updated'] is None]
    actual_bids_to_db = [bid for bid in bids_to_database if bid['last_updated'] is not None]
    actual_bids_to_db = sorted(actual_bids_to_db, key=lambda x: datetime.strptime(x['last_updated'], "%Y-%m-%d "
                                                                                                     "%H:%M:%S"),
                               reverse=True)
    bid_counter = Counter([bid['bid_uri'] for bid in actual_bids_to_db])
    bids_to_db = list()
    for bid_id in bid_counter:
        counter = bid_counter[bid_id]
        if counter > 1:
            bids = [bid for bid in actual_bids_to_db if bid['bid_uri'] == bid_id]
            # The bid list is ordered by date, so the first element will be the most recent one
            most_recent = bids[0]
            if any([bid['storage_mode'] == 'new' for bid in bids]):
                most_recent['storage_mode'] = 'new'
            else:
                most_recent['storage_mode'] = 'update'
        else:
            most_recent = [bid for bid in actual_bids_to_db if bid['bid_uri'] == bid_id][0]
        bids_to_db.append(most_recent)
    bid_counter = Counter(bid['bid_uri'] for bid in bids_to_db)
    if any([bid_counter[counter] > 1 for counter in bid_counter]):
        print('Problem')
    all_bids_to_db = deleted_bids_to_db + bids_to_db  # Possition deletion date before so that it is always the
    # first value
    bid_counter = Counter(bid['bid_uri'] for bid in all_bids_to_db)
    bids_to_db = list()
    for bid_id in bid_counter:
        counter = bid_counter[bid_id]
        if counter > 1:
            bids = [bid for bid in all_bids_to_db if bid['bid_uri'] == bid_id]
            del_bid = {k: v for k, v in bids[0].items() if v is not None}
            actual_bid = bids[1]
            most_recent = {**actual_bid, **del_bid}
            if any([bid['storage_mode'] == 'new' for bid in bids]):
                most_recent['storage_mode'] = 'new'
            else:
                most_recent['storage_mode'] = 'update'
        else:
            most_recent = [bid for bid in all_bids_to_db if bid['bid_uri'] == bid_id][0]
        bids_to_db.append(most_recent)
    bid_counter = Counter(bid['bid_uri'] for bid in bids_to_db)
    if any([bid_counter[counter] > 1 for counter in bid_counter]):
        print('Problem')
    return bids_to_db


def parse_rfc3339_time(date):
    date_obj = iso8601.parse_date(date)
    date = date_obj.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    offset = str(date_obj.tzinfo)
    return date, offset


def get_code_info(url, mapping):
    if not mapping.get(url, ''):
        response = requests.get(url)
        # Check if URL is accessible
        if str(response.status_code)[0] == '4':
            mapping[url] = {'status': 'not_found'}
        else:
            root = etree.fromstring(response.content)
            mapping[url] = dict()
            for row in root.iterfind('SimpleCodeList/Row'):
                for value in row:
                    if value.attrib['ColumnRef'] == 'code':
                        code = value.find('SimpleValue').text
                    elif value.attrib['ColumnRef'] == 'nombre':
                        type = value.find('SimpleValue').text.upper()
                mapping[url][code] = type
    return mapping[url]


def get_full_contractor_name(element):
    this_level = str()
    super_level = str()
    for child in element:
        if 'Name' in child.tag:
            this_level = child.find("Name").text
        else:
            super_level = get_full_contractor_name(child)
    if not super_level:
        return this_level
    elif super_level == this_level:
        return super_level
    else:
        return f'{super_level} > {this_level}'


def clean_atom_elements(root):
    for element in root:
        element.tag = re.sub('{.*}', '', element.tag)
        clean_atom_elements(element)
    return root


def deleted_bid(bid_uri, bid_metadata, bid_info_db):
    """

    :param bid_uri: Bid id
    :param bid_metadata: Bid data
    :param bid_info_db: dict with items stored in database
    :return:
    """
    stored_bids = bid_info_db['bid_uri']  # List of stored bids
    stored_offsets = bid_info_db['deleted_at_offset']  # List of deletion_times
    deleted = False
    bid_metadata['storage_mode'] = 'new'
    if bid_uri in stored_bids:
        db_logger.debug(f'Bid {bid_uri} already stored')
        index = stored_bids.index(bid_uri)
        deletion_date = stored_offsets[index]
        if deletion_date is None:
            db_logger.debug(f'Storing deletion date for bid {bid_uri}')
            deleted = False
            bid_metadata['storage_mode'] = 'update'
        else:
            db_logger.debug(f'Bid {bid_uri} already deleted from PCSP')
            deleted = True
    else:
        db_logger.debug(f'Storing deleted bid {bid_uri}')
    return deleted


def new_bid(bid_uri, bid_metadata, bid_info_db):
    stored_bids = bid_info_db['bid_uri']  # List of stored bids
    if bid_uri in stored_bids:
        bid_metadata['storage_mode'] = 'update'
        return False
    else:
        bid_metadata['storage_mode'] = 'new'
        return True


def more_recent_bid(bid_uri, last_updated, offset, stored_last_update, stored_offset):
    if stored_offset is None:  # This means that the bid appears as deleted but there is not actual data for the bid
        return True
    else:
        if stored_offset != offset:  # If different offsets, transform times to UTC
            hours, minutes = offset.split(':')
            stored_hours, stored_minutes = stored_offset.split(':')
            hours = int(hours)
            stored_hours = int(stored_hours)
            minutes = int(minutes)
            stored_minutes = int(stored_minutes)
            last_update = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S") - timedelta(hours=hours, minutes=minutes)
            stored_last_update = datetime.strptime(str(stored_last_update), "%Y-%m-%d %H:%M:%S") - timedelta(
                hours=stored_hours, minutes=stored_minutes)
        else:
            last_update = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")
            stored_last_update = datetime.strptime(str(stored_last_update), "%Y-%m-%d %H:%M:%S")
        if last_update > stored_last_update:
            db_logger.debug(f'Bid {bid_uri} is more recent than stored entry. Updating bid...')
            return True
        else:
            return False