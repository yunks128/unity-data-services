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
        data_only_assets = []
        for each_granule_asset in assets:
            data_only_asset = {k: v.to_dict() for k, v in each_granule_asset.items() if v.roles is not None and 'data' in v.roles}
            data_only_assets.append(data_only_asset)
        # assets = [k for k in assets if 'roles' in k and 'data' in k['role']]
        # data_assets = [{'assets': {'data': k['data'].to_dict()}} for k in assets if 'data' in k]
        return data_only_assets
