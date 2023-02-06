from cumulus_lambda_functions.stage_in_out.search_collections_abstract import SearchCollectionsAbstract


class SearchCollectionsCmr(SearchCollectionsAbstract):
    def search(self, **kwargs):
        raise NotImplementedError()
