from pystac import Catalog, Item, Asset

from cumulus_lambda_functions.lib.utils.file_utils import FileUtils


class GranulesCatalog:

    def get_child_link_hrefs(self, catalog_file_path: str):
        if not FileUtils.file_exist(catalog_file_path):
            raise ValueError(f'missing file: {catalog_file_path}')
        catalog = FileUtils.read_json(catalog_file_path)
        catalog = Catalog.from_dict(catalog)
        return [k.href for k in catalog.get_links(rel='child')]

    def get_granules_item(self, granule_stac_json) -> Item:
        if not FileUtils.file_exist(granule_stac_json):
            raise ValueError(f'missing file: {granule_stac_json}')
        granules_stac = FileUtils.read_json(granule_stac_json)
        granules_stac = Item.from_dict(granules_stac)
        return granules_stac

    def extract_assets_href(self, granules_stac: Item) -> dict:
        assets = {k: v.href for k, v in granules_stac.get_assets().items()}
        return assets

    def update_assets_href(self, granules_stac: Item,  new_assets: dict):
        for k, v in new_assets.items():
            if k in granules_stac.assets:
                existing_asset = granules_stac.assets.get(k)
                existing_asset.href = v
            else:
                existing_asset = Asset(v, k)
            granules_stac.add_asset(k, existing_asset)
        return self
