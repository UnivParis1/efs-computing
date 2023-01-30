import logging

import requests


class HalApiClient:
    HAL_API_URL = "https://api.archives-ouvertes.fr/search/halshs/?"

    FACET_SEP = '_FacetSep_'
    JOIN_SEP = '_JoinSep_'

    LIST_QUERY_TEMPLATE = "q=docType_s:(ART OR OUV OR COUV OR COMM OR THESE OR HDR OR REPORT OR NOTICE OR PROCEEDINGS)" \
                          "&cursorMark=%%CURSOR%%" \
                          "&sort=docid asc&rows=%%ROWS%%" \
                          "&fq=instStructAcronym_sci:UP1 %%DATE_INTERVAL%%" \
                          "&fl=docid,fr_title_s,en_title_s,fr_subTitle_s,en_subTitle_s,fr_abstract_s,en_abstract_s," \
                          "fr_keyword_s,en_keyword_s," \
                          "authIdForm_i,authFullNameFormIDPersonIDIDHal_fs,docType_s," \
                          "ePublicationDate_s,citationFull_s,citationRef_s," \
                          "authIdHasStructure_fs, labStructId_i"
    DATE_INTERVAL_TEMPLATE = "AND (submittedDate_tdate:[NOW-%%DAYS%%DAYS/DAY TO NOW/HOUR] " \
                             "OR modifiedDate_tdate:[NOW-%%DAYS%%DAYS/DAY TO NOW/HOUR])"

    def __init__(self, days: int, rows: int, logger: logging.Logger) -> None:
        self.rows = rows
        self.logger = logger
        self.date_interval = HalApiClient.DATE_INTERVAL_TEMPLATE.replace('%%DAYS%%',
                                                                         str(days)) if days is not None else ''
        self.cursor = "*"
        self.total = None

    def fetch_publications(self) -> list:
        """


        """
        json_request_string = HalApiClient.HAL_API_URL + HalApiClient.LIST_QUERY_TEMPLATE \
            .replace('%%CURSOR%%',
                     str(self.cursor)) \
            .replace('%%ROWS%%', str(self.rows)) \
            .replace('%%DATE_INTERVAL%%', self.date_interval)
        self.logger.debug(f"Request to HAL : {json_request_string}")
        response = requests.get(json_request_string, timeout=360)
        json_response = response.json()
        if 'error' in response.json().keys():
            raise Exception(f"Error response from HAL API for request : {json_request_string}")
        if not self.total:
            self.total = int(json_response['response']['numFound'])
            self.logger.info(f"{self.total} entries")
        self.cursor = json_response['nextCursorMark']
        return self.cursor, json_response['response']['docs']
