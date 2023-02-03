
from cumulus_lambda_functions.stage_in_out.search_granules_abstract import SearchGranulesAbstract


class SearchGranulesCmr(SearchGranulesAbstract):
    def search(self, **kwargs) -> list:
        raise NotImplementedError()
