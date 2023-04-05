from cumulus_lambda_functions.cumulus_stac.item_transformer import ItemTransformer


class StacUtils:
    @staticmethod
    def reduce_stac_list_to_data_links(input_stac_granules_list: [dict]):
        """
        :param input_stac_granules_list: list of pystac dictionaries
        :return: [dict] - list of assets which has data key
        """
        pystac_items = [ItemTransformer().from_stac(k) for k in input_stac_granules_list]
        assets = [k.get_assets() for k in pystac_items]
        data_assets = [{'assets': {'data': k['data'].to_dict()}} for k in assets if 'data' in k]
        return data_assets
