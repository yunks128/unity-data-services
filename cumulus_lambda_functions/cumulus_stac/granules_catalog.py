import os

from pystac import Catalog, Item, Asset

from cumulus_lambda_functions.lib.utils.file_utils import FileUtils


class GranulesCatalog:

    def get_child_link_hrefs(self, catalog_file_path: str):
        if not FileUtils.file_exist(catalog_file_path):
            raise ValueError(f'missing file: {catalog_file_path}')
        catalog = FileUtils.read_json(catalog_file_path)
        catalog = Catalog.from_dict(catalog)
        child_links = [k.href for k in catalog.get_links(rel='child')]
        catalog_dir = os.path.dirname(catalog_file_path)
        new_child_links = []
        for each_link in child_links:
            if not FileUtils.is_relative_path(each_link):
                new_child_links.append(each_link)
                continue
            new_child_links.append(os.path.join(catalog_dir, each_link))
        return new_child_links

    def get_granules_item(self, granule_stac_json) -> Item:
        if not FileUtils.file_exist(granule_stac_json):
            raise ValueError(f'missing file: {granule_stac_json}')
        granules_stac = FileUtils.read_json(granule_stac_json)
        granules_stac = Item.from_dict(granules_stac)
        return granules_stac

    def extract_assets_href(self, granules_stac: Item, dir_name: str = '') -> dict:
        try:
            self_dir = os.path.dirname(granules_stac.self_href)
        except:
            self_dir = None
        assets = {}
        for k, v in granules_stac.get_assets().items():
            href = v.href
            if not FileUtils.is_relative_path(href):
                assets[k] = href
                continue
            if dir_name is not None and len(dir_name) > 0:
                assets[k] = os.path.join(dir_name, href)
                continue
            if self_dir is not None and len(self_dir) > 0:
                assets[k] = os.path.join(self_dir, href)
                continue
            assets[k] = href
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
