import xml.etree
from xml.etree import ElementTree
import requests
import re
import iso8601
import pytz
from database_generator.info_storage import is_stored
from database_generator.info_storage import is_new_or_update
from database_generator.info_storage import is_deleted


def get_next_link(root):
    root = clean_elements(root)
    for link in root.findall('link'):
        if link.attrib['rel'] == 'next':
            next_link = link.attrib['href']
            break
    return next_link, root


def process_xml_atom(root, manager):
    stored_data = manager[0]
    gc_dict = manager[1]
    # CLEAN ELEMENTS
    answers_set = list()
    for link in root.findall('link'):
        if link.attrib['rel'] == 'self':
            this_link = link.attrib['href']
            print(f"XML being processed: {this_link}")
            break
    # Get information for deleted entries
    for deleted_entry in root.findall('deleted-entry'):
        id = deleted_entry.attrib['ref']
        deletion_date = deleted_entry.attrib['when']
        deletion_date, offset = parse_rfc3339_time(deletion_date)
        bid_metadata = {'bid_uri': id, 'deleted_at': deletion_date, 'deleted_at_offset': offset, 'table_name': 'bids'}
        if not is_deleted(id, bid_metadata, stored_data):
            answers_set.append(bid_metadata)
    print(len(root.findall('entry')) + len(root.findall('deleted-entry')))
    for entry in root.findall('entry'):
        bid_metadata = {"table_name": "bids"}
        # Get mandatory info for bid
        id = entry.find('id').text  # Unique ID
        last_updated = entry.find('updated').text
        last_updated, offset = parse_rfc3339_time(last_updated)
        if not is_new_or_update(id, last_updated, offset, bid_metadata, stored_data):
            continue
        xmlstr = ElementTree.tostring(entry, encoding='utf8', method='xml').decode('utf8')
        # TODO: Remove after checking everything is OK. DEBUG FOR NON ANALYZED FIELDS
        stop_terms = ['SubmissionMethod', 'Event>', 'ProcessJustification', '<SubcontractTerms', 'DocumentProviderParty',
                      'TenderRecipientParty', 'AdditionalInformationParty', 'Appeal', 'TenderPreparation']
        printed = False
        for term in stop_terms:
            if term in xmlstr:
                if not printed:
                    print(xmlstr)
                    printed = True
                print(f'Found term: {term}')
        # Check duplicate tags TODO: Remove when all is OK
        tags = re.findall('<[^/].*?>', xmlstr.replace('/>', '>'))
        for index, tag in enumerate(tags):
            tags[index] = re.sub(' .*', '', tag)
            if tags[index][-1] != '>':
                tags[index] += '>'
        # TODO: Name tag in country
        known_dup_tags = ['<PartyName>', '<Name>', '<ParentLocatedParty>', '<AdditionalPublicationDocumentReference>',
                          '<IssueDate>', '<ID>', '<Attachment>', '<ExternalReference>', '<URI>',
                          '<AdditionalPublicationStatus>', '<PublicationMediaName>',
                          '<RequiredCommodityClassification>', '<ItemClassificationCode>', '<ValidNoticeInfo>',
                          '<NoticeTypeCode>', '<TaxExclusiveAmount>', '<TenderResult>', '<ResultCode>',
                          '<ReceivedTenderQuantity>', '<WinningParty>', '<PartyIdentification>', '<EndDate>',
                          '<ProcurementProjectLotID>', '<LegalMonetaryTotal>', '<Country>', '<IdentificationCode>',
                          '<StartDate>', '<AwardedTenderedProject>', '<EndTime>', '<Description>',
                          '<SpecificTendererRequirement>', '<RequirementTypeCode>', '<DocumentHash>',
                          '<AdditionalDocumentReference>', '<AwardingCriteria>', '<EvaluationCriteriaTypeCode>',
                          '<WeightNumeric>', '<ProcurementProject>', '<BudgetAmount>', '<ProcurementProjectLot>',
                          '<RequiredFinancialGuarantee>', '<GuaranteeTypeCode>', '<TotalAmount>', '<AwardDate>',
                          '<LowerTenderAmount>', '<HigherTenderAmount>', '<PayableAmount>', '<Contract>',
                          '<AmountRate>', '<TechnicalEvaluationCriteria>', '<FinancialEvaluationCriteria>',
                          '<ContractModification>', '<Note>', '<ContractID>',
                          '<ContractModificationLegalMonetaryTotal>', '<FinalLegalMonetaryTotal>', '<CityName>',
                          '<PostalZone>', '<ClassificationCategory>', '<CodeValue>', '<LiabilityAmount>',
                          '<ContractModificationDurationMeasure>', '<FinalDurationMeasure>']
        dup_tags = [tag for tag in tags if tags.count(tag) > 1 and tag not in known_dup_tags]
        if dup_tags:
            print(dup_tags)
            print(xmlstr)
        bid_metadata['bid_uri'] = id
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
        status_dict = code_mapper(url_bid_status, gc_dict)
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
                    bid_metadata['fecha_inicio'] = start_date.text
                end_date = planned_period.find('EndDate')
                if end_date is not None:
                    bid_metadata['fecha_fin'] = end_date.text
            ## CODIGO CPV (0-N)
            for code in procurement_project.findall('RequiredCommodityClassification'):
                cpv_metadata = {'table_name': 'bid_cpv_codes'}
                cpv_metadata['bid_id'] = bid_metadata['bid_uri']
                code_element = code.find('ItemClassificationCode')
                cpv_code = code_element.text
                cpv_metadata['code'] = cpv_code
                url_cpv_code = code_element.attrib['listURI']
                code_dict = code_mapper(url_cpv_code, gc_dict)
                cpv_metadata['code_description'] = code_dict.get(cpv_code, '')
                if not is_stored('bid_cpv_codes', cpv_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(cpv_metadata)
            ## TIPO DE CONTRATO (0-1)
            type_element = procurement_project.find('TypeCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                type_dict = code_mapper(url_bid_type, gc_dict)
                bid_metadata['tipo_contrato'] = type_dict.get(type, type)
            ## SUBTIPO DE CONTRATO (0-1)
            subtype_element = procurement_project.find('SubTypeCode')
            if subtype_element is not None:
                code = subtype_element.text
                url_code = subtype_element.attrib['listURI']
                code_dict = code_mapper(url_code, gc_dict)
                bid_metadata['subtipo_contrato'] = f"{code}: {code_dict.get(code, '')}."
            ## EXTENSION DEL CONTRATO
            for contract_extension in procurement_project.findall('ContractExtension'):
                ext_metadata = {'table_name': 'contract_extensions'}
                ext_metadata['bid_id'] = bid_metadata['bid_uri']
                option = contract_extension.find('OptionsDescription')
                if option is not None:
                    ext_metadata['opcion'] = option.text
                validity_period = contract_extension.find('OptionValidityPeriod/Description')
                if validity_period is not None:
                    ext_metadata['periodo_validez'] = validity_period.text
                if not is_stored('contract_extensions', ext_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(ext_metadata)
            ## LUGAR DE EJECUCION
            location = procurement_project.find('RealizedLocation')
            if location is not None:
                county_element = location.find('CountrySubentityCode')
                if county_element is not None:
                    county = county_element.text
                    url_bid_county = county_element.attrib['listURI']
                    county_dict = code_mapper(url_bid_county, gc_dict)
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
                        code_dict = code_mapper(url_codes, gc_dict)
                        bid_metadata['pais_ejecucion'] = code_dict.get(code, code)

        # DOCUMENTOS: Van en otra base de datos (1 licitación - N documentos)
        ## PLIEGO ADMINISTRATIVO (0-1)
        doc_element = status.find('LegalDocumentReference')
        if doc_element is not None:
            doc_metadata = {"table_name": "docs"}
            doc_metadata['bid_id'] = bid_metadata['bid_uri']
            doc_metadata['doc_id'] = doc_element.find('ID').text
            doc_metadata['doc_url'] = doc_element.find('Attachment/ExternalReference/URI').text
            doc_hash = doc_element.find('Attachment/ExternalReference/DocumentHash')
            if doc_hash is not None:
                doc_metadata['doc_hash'] = doc_hash.text
            doc_metadata['doc_type'] = 'administrativo'
            if not is_stored('docs', doc_metadata, bid_metadata['storage_mode'], stored_data):
                answers_set.append(doc_metadata)
        ## PLIEGO TECNICO (0-1)
        doc_element = status.find('TechnicalDocumentReference')
        if doc_element is not None:
            doc_metadata = {"table_name": "docs"}
            doc_metadata['bid_id'] = bid_metadata['bid_uri']
            doc_metadata['doc_id'] = doc_element.find('ID').text
            doc_metadata['doc_url'] = doc_element.find('Attachment/ExternalReference/URI').text
            if doc_element.find('Attachment/ExternalReference/DocumentHash') is not None:
                doc_metadata['doc_hash'] = doc_element.find('Attachment/ExternalReference/DocumentHash').text
            doc_metadata['doc_type'] = 'tecnico'
            if not is_stored('docs', doc_metadata, bid_metadata['storage_mode'], stored_data):
                answers_set.append(doc_metadata)
        ## OTROS DOCUMENTOS (0-N)
        for doc_element in status.findall('AdditionalDocumentReference'):
            doc_metadata = {"table_name": "docs"}
            doc_metadata['bid_id'] = bid_metadata['bid_uri']
            doc_metadata['doc_id'] = doc_element.find('ID').text
            doc_metadata['doc_url'] = doc_element.find('Attachment/ExternalReference/URI').text
            if doc_element.find('Attachment/ExternalReference/DocumentHash') is not None:
                doc_metadata['doc_hash'] = doc_element.find('Attachment/ExternalReference/DocumentHash').text
            doc_metadata['doc_type'] = 'otro'
            if not is_stored('docs', doc_metadata, bid_metadata['storage_mode'], stored_data):
                answers_set.append(doc_metadata)

        # LOTES (1)
        for lot_element in status.findall('ProcurementProjectLot'):
            lot_metadata = {'table_name': 'lots'}
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
            for code in lote_object_element.findall('RequiredCommodityClassification'):
                cpv_metadata = {'table_name': 'lot_cpv_codes'}
                cpv_metadata['bid_id'] = bid_metadata['bid_uri']
                code_element = code.find('ItemClassificationCode')
                cpv_code = code_element.text
                cpv_metadata['code'] = cpv_code
                url_cpv_code = code_element.attrib['listURI']
                code_dict = code_mapper(url_cpv_code, gc_dict)
                cpv_metadata['code_description'] = code_dict.get(cpv_code, '')
                if not is_stored('lot_cpv_codes', cpv_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(cpv_metadata)
            if not is_stored('lots', lot_metadata, bid_metadata['storage_mode'], stored_data):
                answers_set.append(lot_metadata)

        # TENDERING PROCESS
        tendering_process = status.find('TenderingProcess')
        if tendering_process is not None:
            ## TIPO DE PROCEDIMIENTO
            type_element = tendering_process.find('ProcedureCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                type_dict = code_mapper(url_bid_type, gc_dict)
                bid_metadata['tipo_procedimiento'] = type_dict.get(type, type)
            ## SISTEMA DE CONTRATACION
            type_element = tendering_process.find('ContractingSystemCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                type_dict = code_mapper(url_bid_type, gc_dict)
                bid_metadata['sistema_contratacion'] = type_dict.get(type, type)
            ## TIPO DE TRAMITACION
            type_element = tendering_process.find('UrgencyCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                type_dict = code_mapper(url_bid_type, gc_dict)
                bid_metadata['tipo_tramitacion'] = type_dict.get(type, type)
            ## PRESENTACION DE LA OFERTA
            type_element = tendering_process.find('SubmissionMethodCode')
            if type_element is not None:
                type = type_element.text
                url_bid_type = type_element.attrib['listURI']
                type_dict = code_mapper(url_bid_type, gc_dict)
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
                    bid_metadata['plazo_presentacion'] = f"{end_date.text} {end_time.text}"
            ## EVENTOS
            for evento_element in tendering_process.findall('OpenTenderEvent'):
                event_metadata = {"table_name": "events"}
                event_metadata['bid_id'] = bid_metadata['bid_uri']
                event_type = evento_element.find('TypeCode')
                if event_type is not None:
                    type = event_type.text
                    url_event_type = event_type.attrib['listURI']
                    type_dict = code_mapper(url_event_type, gc_dict)
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
                            code_dict = code_mapper(url_country_code, gc_dict)
                            event_metadata['pais'] = code_dict.get(country_code, country_code)
                if not is_stored('events', event_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(event_metadata)
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
                    code_dict = code_mapper(url_code, gc_dict)
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
                org_metadata = {"table_name": "orgs"}
                provider_name = provider_element.find('PartyName/Name')
                org_metadata['nombre'] = provider_name
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
                        code_dict = code_mapper(url_country_code, gc_dict)
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
                if not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(org_metadata)
            ## LUGAR RECEPCION OFERTAS
            receiving_element = tendering_terms.find('TenderRecipientParty')
            if receiving_element is not None:
                org_metadata = {"table_name": "orgs"}
                receiving_name = receiving_element.find('PartyName/Name')
                org_metadata['nombre'] = receiving_name
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
                        code_dict = code_mapper(url_country_code, gc_dict)
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
                if not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(org_metadata)
            ## PROVEEDOR DE INFORMACION ADICIONAL
            provider_element = tendering_terms.find('AdditionalInformationParty')
            if provider_element is not None:
                org_metadata = {"table_name": "orgs"}
                provider_name = provider_element.find('PartyName/Name')
                org_metadata['nombre'] = provider_name
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
                        code_dict = code_mapper(url_country_code, gc_dict)
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
                if not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(org_metadata)
            ## APPEAL TERMS
            appeal_terms = tendering_terms.find('AppealTerms')
            if appeal_terms is not None:
                ### INFORMACION SOBRE RECURSOS
                resource_info_element = appeal_terms.find('AppealInformationParty')
                if resource_info_element is not None:
                    org_metadata = {"table_name": "orgs"}
                    resource_name = resource_info_element.find('PartyName/Name')
                    org_metadata['nombre'] = resource_name
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
                            code_dict = code_mapper(url_country_code, gc_dict)
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
                    if not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data):
                        answers_set.append(org_metadata)
                ### LUGAR DE PRESENTACION DE RECURSOS
                resource_presentation_element = appeal_terms.find('AppealReceiverParty')
                if resource_presentation_element is not None:
                    org_metadata = {"table_name": "orgs"}
                    resource_name = resource_presentation_element.find('PartyName/Name')
                    org_metadata['nombre'] = resource_name
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
                            code_dict = code_mapper(url_country_code, gc_dict)
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
                    if not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data):
                        answers_set.append(org_metadata)
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
                    org_metadata = {"table_name": "orgs"}
                    mediation_name = mediation_element.find('PartyName/Name')
                    org_metadata['nombre'] = mediation_name
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
                            code_dict = code_mapper(url_country_code, gc_dict)
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
                    if not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data):
                        answers_set.append(org_metadata)
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
                code_dict = code_mapper(url_code, gc_dict)
                funding_program_text = code_dict.get(code, code)
            if funding_program is not None:
                funding_program_text += f' {funding_program.text}'
            if funding_program_text:
                bid_metadata['programa_financiacion'] = funding_program_text.strip()
            ## GARANTIAS REQUERIDAS ## TODO: Anotar las divisas
            for guarantee_element in tendering_terms.findall('RequiredFinancialGuarantee'):
                guarantee_metadata = {'table_name': 'required_guarantees'}
                guarantee_metadata['bid_id'] = bid_metadata['bid_uri']
                guarantee_type = guarantee_element.find('GuaranteeTypeCode')
                if guarantee_type is not None:
                    code = guarantee_type.text
                    url_code = guarantee_type.attrib['listURI']
                    code_dict = code_mapper(url_code, gc_dict)
                    guarantee_metadata['tipo_garantia'] = code_dict.get(code, code)
                guarantee_amount = guarantee_element.find('LiabilityAmount')
                if guarantee_amount is not None:
                    guarantee_metadata['importe_garantia'] = guarantee_amount.text
                guarantee_rate = guarantee_element.find('AmountRate')
                if guarantee_rate is not None:
                    guarantee_metadata['porcentaje_garantia'] = guarantee_rate.text
                if not is_stored('required_guarantees', guarantee_metadata, bid_metadata['storage_mode'], stored_data):
                    answers_set.append(guarantee_metadata)
            ## REQUISITOS DE PARTICIPACION
            requisites_element = tendering_terms.find('TendererQualificationRequest')
            if requisites_element is not None:
                ### TITULO HABILITANTE
                personal_situation = requisites_element.find('PersonalSituation')
                if personal_situation is not None:
                    bid_metadata['titulo_habilitante_participacion'] = personal_situation.text
                ### DESCRIPCION
                description = tendering_terms.find('Description')
                if description is not None:
                    bid_metadata['descripcion_requisitos_participacion'] = description.text
                ### CLASIFICACION EMPRESARIAL
                for required_bussiness_class in requisites_element.findall(
                        'RequiredBusinessClassificationScheme/ClassificationCategory'):
                    class_metadata = {'table_name': 'required_business_classifications'}
                    class_metadata['bid_id'] = bid_metadata['bid_uri']
                    description = required_bussiness_class.find('Description')
                    if description is not None:
                        class_metadata['clasificacion_empresarial'] = required_bussiness_class
                    code = required_bussiness_class.find('CodeValue')
                    if code is not None:
                        class_metadata['codigo_clasificacion_empresarial'] = code.text
                    if not is_stored('required_business_classifications', class_metadata, bid_metadata[
                        'storage_mode'], stored_data):
                        answers_set.append(class_metadata)

                ### CONDICIONES DE ADMISION
                for condition in requisites_element.findall('SpecificTendererRequirement/RequirementTypeCode'):
                    cond_metadata = {'table_name': 'admission_conditions'}
                    cond_metadata['bid_id'] = bid_metadata['bid_uri']
                    code = condition.text
                    url_code = condition.attrib['listURI']
                    code_dict = code_mapper(url_code, gc_dict)
                    cond_metadata['condicion'] = code_dict.get(code, code)
                    answers_set.append(cond_metadata)
                ### CRITERIO DE EVALUACION TECNICA
                for tech_criteria in requisites_element.findall('TechnicalEvaluationCriteria'):
                    crit_metadata = {'table_name': 'evaluation_criteria'}
                    crit_metadata['bid_id'] = bid_metadata['bid_uri']
                    crit_metadata['tipo_criterio'] = 'TECNICO'
                    criteria_code = tech_criteria.find('EvaluationCriteriaTypeCode')
                    if criteria_code is not None:
                        code = criteria_code.text
                        url_code = criteria_code.attrib['listURI']
                        code_dict = code_mapper(url_code, gc_dict)
                        crit_metadata['codigo_criterio'] = code_dict.get(code, code)
                    criteria_description = tech_criteria.find('Description')
                    if criteria_description is not None:
                        crit_metadata['descripcion_criterio'] = criteria_description.text
                    if not is_stored('evaluation_criteria', crit_metadata, bid_metadata['storage_mode'], stored_data):
                        answers_set.append(crit_metadata)
                ### CRITERIO DE EVALUACION ECONOMICO-FINANCIERA
                for finantial_criteria in requisites_element.findall('FinancialEvaluationCriteria'):
                    crit_metadata = {'table_name': 'evaluation_criteria'}
                    crit_metadata['bid_id'] = bid_metadata['bid_uri']
                    crit_metadata['tipo_criterio'] = 'ECONOMICO-FINANCIERO'
                    criteria_code = finantial_criteria.find('EvaluationCriteriaTypeCode')
                    if criteria_code is not None:
                        code = criteria_code.text
                        url_code = criteria_code.attrib['listURI']
                        code_dict = code_mapper(url_code, gc_dict)
                        crit_metadata['codigo_criterio'] = code_dict.get(code, code)
                    criteria_description = finantial_criteria.find('Description')
                    if criteria_description is not None:
                        crit_metadata['descripcion_criterio'] = criteria_description.text
                    if not is_stored('evaluation_criteria', crit_metadata, bid_metadata['storage_mode'], stored_data):
                        answers_set.append(crit_metadata)
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
                    code_dict = code_mapper(url_code, gc_dict)
                    bid_metadata['tipo_documento_sobre'] = code_dict.get(code, code)
                envelope_description = tender_preparation.find('Description')
                if envelope_description is not None:
                    bid_metadata['descripcion_preparacion_oferta'] = envelope_description.text
            ## CONDICIONES ADJUDICACION
            for awarding_terms in tendering_terms.findall('AwardingTerms/AwardingCriteria'):
                condition_metadata = {'table_name': 'awarding_conditions'}
                condition_metadata['bid_id'] = bid_metadata['bid_uri']
                term_description = awarding_terms.find('Description')
                if term_description is not None:
                    condition_metadata['criterio_adjudicacion'] = term_description.text
                weight = awarding_terms.find('WeightNumeric')
                if weight is not None:
                    condition_metadata['ponderacion_adjudicacion'] = weight.text
                answers_set.append(condition_metadata)

        # RESULTADO PROCEDIMIENTO
        for result in status.findall('TenderResult'):
            winner_metadata = {"table_name": "winners"}
            winner_metadata['bid_id'] = bid_metadata['bid_uri']
            code_element = result.find('ResultCode')
            if code_element is not None:
                code = code_element.text
                url_code = code_element.attrib['listURI']
                code_dict = code_mapper(url_code, gc_dict)
                ## ESTADO FINAL ADJUDIACION
                winner_metadata['resultado'] = code_dict.get(code, code)
            ## ADJUDICATARIO
            winner = result.find('WinningParty')
            if winner is not None:
                org_metadata = {"table_name": "orgs"}
                winner_metadata['adjudicatario'] = winner.find('PartyName/Name').text
                org_metadata['nombre'] = winner_metadata['adjudicatario']
                winner_id = winner.find('PartyIdentification/ID')
                org_metadata['id'] = winner_id.text
                org_metadata['razon_social'] = winner_id.attrib['schemeName']
            awarded_tendered_project = result.find('AwardedTenderedProject')
            if awarded_tendered_project is not None:
                ### IMPORTE ADJUDICACION
                amount = awarded_tendered_project.find('LegalMonetaryTotal')
                if amount is not None:
                    amount_wo_tax = amount.find('TaxExclusiveAmount')
                    if amount_wo_tax is not None:
                        winner_metadata['importe_sin_impuestos'] = amount_wo_tax.text
                    amount_w_tax = amount.find('PayableAmount')
                    if amount_w_tax is not None:
                        winner_metadata['importe_con_impuestos'] = amount_w_tax.text
                ### ID LOTE
                lot_id = awarded_tendered_project.find('ProcurementProjectLotID')
                if lot_id is not None:
                    winner_metadata['id_lote'] = lot_id.text
            ## NUMERO DE PARTICIPANTES
            num_licitadores = result.find('ReceivedTenderQuantity')
            if num_licitadores is not None:
                winner_metadata['numero_participantes'] = num_licitadores.text
            ## MOTIVACION
            motivation = result.find('Description')
            if motivation is not None:
                winner_metadata['motivacion'] = motivation.text
            ## FECHA
            date = result.find('AwardDate')
            if date is not None:
                winner_metadata['fecha_adjudicacion'] = date.text
            ## OFERTAS RECIBIDAS
            highest_offer = result.find('HigherTenderAmount')
            if highest_offer is not None:
                winner_metadata['maxima_oferta_recibida'] = highest_offer.text
            lowest_offer = result.find('LowerTenderAmount')
            if lowest_offer is not None:
                winner_metadata['minima_oferta_recibida'] = lowest_offer.text
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
                    winner_metadata['fecha_formalizacion'] = issue_date.text
            ## FECHA DE ENTRAGA EN VIGOR DEL CONTRATO
            entrada_vigor = result.find('StartDate')
            if entrada_vigor is not None:
                winner_metadata['fecha_entrada_en_vigor'] = entrada_vigor.text
            ## CONDICIONES DE SUBCONTRATACION
            subcontract_terms = result.find('SubcontractTerms')
            if subcontract_terms is not None:
                rate = subcontract_terms.find('Rate')
                if rate is not None:
                    winner_metadata['porcentaje_subcontratacion'] = rate.text
                subcontract_description = subcontract_terms.find('Description')
                if subcontract_description is not None:
                    winner_metadata['objeto_subcontratacion'] = subcontract_description.text
            if not is_stored('winners', winner_metadata, bid_metadata['storage_mode'], stored_data):
                answers_set.append(winner_metadata)
        # ORGANO DE CONTRATACION
        org_metadata = {"table_name": "orgs"}
        located_contracting_party = status.find('LocatedContractingParty')
        upper_levels = str()
        org_element = located_contracting_party.find('Party')
        lowest_level = org_element.find('PartyName/Name').text
        org_metadata['nombre'] = lowest_level
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
            code_dict = code_mapper(url_code, gc_dict)
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
                code_dict = code_mapper(url_country_code, gc_dict)
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
        if len(org_metadata) > 2 and not is_stored('orgs', org_metadata, bid_metadata['storage_mode'], stored_data,
                                                   answers_set):
            answers_set.append(org_metadata)
        hiera_element = located_contracting_party.find('ParentLocatedParty')
        if hiera_element is not None:
            upper_levels = iterate_parent_contractor(hiera_element)
        if upper_levels:
            bid_metadata['organo_de_contratacion'] = f'{upper_levels} > {lowest_level}'
        else:
            bid_metadata['organo_de_contratacion'] = lowest_level
        # MODIFICACIONES DE CONTRATO
        for mod in status.findall('ContractModification'):
            mod_metadata = {"table_name": "contract_mods"}
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
            if not is_stored('contract_mods', mod_metadata, bid_metadata['storage_mode'], stored_data):
                answers_set.append(mod_metadata)
        # MEDIOS DE PUBLICACION
        for medium in status.findall('ValidNoticeInfo'):
            medium_metadata = dict()
            medium_metadata['table_name'] = "publications"
            medium_metadata['bid_id'] = bid_metadata['bid_uri']
            add_type = medium.find('NoticeTypeCode')
            if add_type is not None:
                code = add_type.text
                url_code = add_type.attrib['listURI']
                code_dict = code_mapper(url_code, gc_dict)
                medium_metadata['tipo_anuncio'] = code_dict.get(code, code)
            ## Puede haber varios medios de publicacion para un mismo tipo de anuncio
            for pub_status in medium.findall('AdditionalPublicationStatus'):
                copy_pub_metadata = medium_metadata.copy()
                media_name = pub_status.find('PublicationMediaName')
                if media_name is not None:
                    copy_pub_metadata['medio_publicacion'] = media_name.text
                for pub_date in pub_status.findall('AdditionalPublicationDocumentReference/IssueDate'):
                    copy2_pub_metadata = copy_pub_metadata.copy()
                    copy2_pub_metadata['fecha_publicacion'] = pub_date.text
                    if not is_stored('publications', copy2_pub_metadata, bid_metadata['storage_mode'], stored_data):
                        answers_set.append(copy2_pub_metadata)
        answers_set.append(bid_metadata)
    manager[1] = gc_dict
    return answers_set


def parse_rfc3339_time(date):
    date_obj = iso8601.parse_date(date)
    date = date_obj.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    offset = str(date_obj.tzinfo)
    return date, offset


def code_mapper(url, mapping):
    if not mapping.get(url, ''):
        try:
            response = requests.get(url)
        except:
            print('error con url')
        # Check if URL is accessible
        if str(response.status_code)[0] == '4':
            mapping[url] = {'status': 'not_found'}
        else:
            root = xml.etree.ElementTree.fromstring(response.content)
            mapping[url] = dict()
            for row in root.findall('SimpleCodeList/Row'):
                for value in row:
                    if value.attrib['ColumnRef'] == 'code':
                        code = value.find('SimpleValue').text
                    elif value.attrib['ColumnRef'] == 'nombre':
                        type = value.find('SimpleValue').text.upper()
                mapping[url][code] = type
    return mapping[url]


def iterate_parent_contractor(element):
    this_level = str()
    super_level = str()
    for child in element:
        if 'Name' in child.tag:
            this_level = child.find("Name").text
        else:
            super_level = iterate_parent_contractor(child)
    if not super_level:
        return this_level
    elif super_level == this_level:
        return super_level
    else:
        return f'{super_level} > {this_level}'


def clean_elements(root):
    for element in root:
        element.tag = re.sub('{.*}', '', element.tag)
        clean_elements(element)
    return root
