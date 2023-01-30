import requests


class HalApiClient:
    HAL_API_URL = "https://api.archives-ouvertes.fr/search/halshs/?"

    LIST_QUERY_TEMPLATE = "q=docType_s:(ART OR OUV OR COUV OR COMM OR THESE OR HDR OR REPORT OR NOTICE OR PROCEEDINGS)" \
                          "&cursorMark=%%CURSOR%%" \
                          "&sort=docid asc&rows=%%ROWS%%" \
                          "&fq=instStructAcronym_sci:UP1 %%DATE_INTERVAL%%" \
                          "&fl=docid,fr_title_s,en_title_s,fr_subTitle_s,en_subTitle_s,fr_abstract_s,en_abstract_s," \
                          "fr_keyword_s,en_keyword_s,structId_i,authIdHal_i,authIdHal_s,authLastNameFirstName_s," \
                          "authIdForm_i,docType_s,ePublicationDate_s,citationFull_s,citationRef_s" \
                          "authIdHasStructure_fs, labStructId_i,authStructId_i,instStructAcronym_s,labStructAcronym_s," \
                          "structAcronym_s"
    DATE_INTERVAL_TEMPLATE = "AND (submittedDate_tdate:[NOW-%%DAYS%%DAYS/DAY TO NOW/HOUR] " \
                             "OR modifiedDate_tdate:[NOW-%%DAYS%%DAYS/DAY TO NOW/HOUR])"

    def __init__(self, days: int, rows: int, verbose: bool) -> None:
        self.rows = rows
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
        print(f"Request to HAL : {json_request_string}")
        response = requests.get(json_request_string, timeout=360)
        json_response = response.json()
        if 'error' in response.json().keys():
            raise Exception(f"Error response from HAL API for request : {json_request_string}")
        if not self.total:
            self.total = int(json_response['response']['numFound'])
            print(f"{self.total} entries")
        self.cursor = json_response['nextCursorMark']
        return self.cursor, json_response['response']['docs']
