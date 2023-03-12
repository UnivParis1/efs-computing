import logging

import requests


class HalApiClient:
    HAL_API_URL = "https://api.archives-ouvertes.fr/search/paris1/?"

    FACET_SEP = '_FacetSep_'
    JOIN_SEP = '_JoinSep_'

    FILTERED_QUERY = "docType_s:(ART OR OUV OR COUV OR COMM OR THESE OR HDR OR REPORT OR NOTICE OR PROCEEDINGS)"
    NOT_FILTERED_QUERY = "*:*"

    LIST_QUERY_TEMPLATE = "q=%%FILTER%%" \
                          "&cursorMark=%%CURSOR%%" \
                          "&sort=docid asc&rows=%%ROWS%%" \
                          "&fq=instStructAcronym_sci:UP1 %%DATE_INTERVAL%%" \
                          "&fl=docid,fr_title_s,en_title_s,fr_subTitle_s,en_subTitle_s,fr_abstract_s,en_abstract_s," \
                          "fr_keyword_s,en_keyword_s," \
                          "authIdForm_i,authFullNameFormIDPersonIDIDHal_fs,docType_s," \
                          "publicationDate_tdate,citationFull_s,citationRef_s," \
                          "authIdHasStructure_fs, labStructId_i"
    ALL_IDS_QUERY_TEMPLATE = "q*:*" \
                             "&sort=docid asc&rows=%%ROWS%%" \
                             "&fq=instStructAcronym_sci:UP1" \
                             "&cursorMark=%%CURSOR%%" \
                             "&wt=json" \
                             "&fl=docid"
    DATE_INTERVAL_TEMPLATE = "AND (submittedDate_tdate:[NOW-%%DAYS%%DAYS/DAY TO NOW/HOUR] " \
                             "OR modifiedDate_tdate:[NOW-%%DAYS%%DAYS/DAY TO NOW/HOUR])"

    def __init__(self, rows: int, logger: logging.Logger, days: int = None, filtered: bool = False) -> None:
        self.rows = rows
        self.logger = logger
        self.filter = self.FILTERED_QUERY if filtered else self.NOT_FILTERED_QUERY
        self.date_interval = HalApiClient.DATE_INTERVAL_TEMPLATE.replace('%%DAYS%%',
                                                                         str(days)) if days is not None else ''
        self.cursor = "*"
        self.total = None

    def _fetch_publications(self, json_request_string: str) -> list:
        self.logger.debug(f"Request to HAL : {json_request_string}")
        response = requests.get(json_request_string, timeout=360)
        json_response = response.json()
        if 'error' in response.json().keys():
            raise Exception(f"Error response from HAL API for request : {json_request_string}")
        if not self.total:
            self.total = int(json_response['response']['numFound'])
            self.logger.info(f"{self.total} entries")
        self.cursor = json_response['nextCursorMark']
        return json_response['response']['docs']

    def fetch_last_publications(self) -> list:
        json_request_string = HalApiClient.HAL_API_URL + HalApiClient.LIST_QUERY_TEMPLATE \
            .replace('%%CURSOR%%',
                     str(self.cursor)) \
            .replace('%%ROWS%%', str(self.rows)) \
            .replace('%%FILTER%%', str(self.filter)) \
            .replace('%%DATE_INTERVAL%%', self.date_interval)
        return self._fetch_publications(json_request_string)

    def fetch_all_publication_ids(self) -> list:
        json_request_string = HalApiClient.HAL_API_URL + HalApiClient.ALL_IDS_QUERY_TEMPLATE \
            .replace('%%CURSOR%%',
                     str(self.cursor)) \
            .replace('%%ROWS%%', str(self.rows))
        return self._fetch_publications(json_request_string)
