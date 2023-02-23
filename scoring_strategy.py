class ScoringStrategy:
    MIN_PRECISION = 0.05
    MAX_PRECISION = 0.7

    @staticmethod
    def apply_limits(precision):
        return min(ScoringStrategy.MAX_PRECISION, max(ScoringStrategy.MIN_PRECISION, float(precision)))

    @staticmethod
    def compute_score(distance, precision):
        return (precision - distance) / precision

    @staticmethod
    def avg(scores_list):
        return sum(scores_list) / len(scores_list)

    def compute_scores_by_author(self, results, precision):
        precision = self.apply_limits(precision)
        inverted_results = {}
        for sent in results:
            distance = sent['_additional']['distance']
            if distance > precision:
                continue
            sent_score = self.compute_score(distance, precision)
            docid = sent['docid']
            sentid = sent['sentid']
            sent_data = {'text': sent['text'], 'score': sent_score, 'id': sentid}
            print(f"{distance} ({docid}) -> {sent['text']} (score : {sent_score}")
            if sent['hasPublication'] is None:
                continue
            pub = sent['hasPublication'][0]
            pub_data = {key: str(pub[key]) if pub[key] is not None else '' for key in
                        ['citation_full', 'citation_ref', 'doc_type', 'docid', 'en_abstract', 'en_keyword', 'en_title',
                         'fr_abstract', 'fr_keyword', 'fr_title']}
            if pub['hasAuthors'] is None:
                continue
            for auth in pub['hasAuthors']:
                author_identifier = auth['identifier']
                auth_data = {key: str(auth[key]) if auth[key] is not None else '' for key in
                             ['identifier', 'name', 'own_inst']}
                if author_identifier not in inverted_results.keys():
                    inverted_results[author_identifier] = auth_data | {'pubs': {}, 'score': 0, 'max_score': 0,
                                                                       'avg_score': 0, 'scores_for_avg': [],
                                                                       'min_dist': 2.0,
                                                                       'avg_dist': 0, 'dist_for_avg': []}
                if docid not in inverted_results[author_identifier]['pubs'].keys():
                    inverted_results[author_identifier]['pubs'][docid] = pub_data | {'score': 0, 'sents': {}}
                inverted_results[author_identifier]['pubs'][docid]['score'] += sent_score
                inverted_results[author_identifier]['score'] += sent_score
                inverted_results[author_identifier]['max_score'] = max(sent_score,
                                                                       inverted_results[author_identifier]['max_score'])
                inverted_results[author_identifier]['min_dist'] = min(distance,
                                                                      inverted_results[author_identifier]['min_dist'])
                inverted_results[author_identifier]['scores_for_avg'].append(sent_score)
                inverted_results[author_identifier]['dist_for_avg'].append(distance)
                inverted_results[author_identifier]['avg_scores'] = self.avg(
                    inverted_results[author_identifier]['scores_for_avg'])
                inverted_results[author_identifier]['avg_dist'] = self.avg(
                    inverted_results[author_identifier]['dist_for_avg'])
                if sentid not in inverted_results[author_identifier]['pubs'][docid]['sents'].keys():
                    inverted_results[author_identifier]['pubs'][docid]['sents'][sentid] = sent_data | {
                        'score': sent_score}
        return inverted_results
