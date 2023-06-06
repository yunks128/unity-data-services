alias_pointer = {
    "actions" : [
        {"add" : {"index" : "authorization_mappings_v1", "alias" : "authorization_mappings"}},
        {"add" : {"index" : "unity_collections_v1", "alias" : "unity_collections"}}
    ]
}

authorization_mappings_v1 = {
  "settings" : {
        "number_of_shards" : 3,
        "number_of_replicas" : 2
    },
  "mappings": {
    "properties": {
      "action": {"type": "keyword"},
      "collection_map": {"type": "keyword"},
      "user_group": {"type": "keyword"},
      "tenant": {"type": "keyword"},
      "tenant_venue": {"type": "keyword"}
    }
  }
}

unity_collections_v1 = {
  "settings" : {
        "number_of_shards" : 3,
        "number_of_replicas" : 2
    },
  "mappings": {
    "properties": {
      "collection_id": {"type": "keyword"},
      "bbox": {"type": "geo_shape"},
      "granule_count": {"type":  "integer"},
      "start_time": {"type":  "long"},
      "end_time": {"type":  "long"}
    }
  }
}