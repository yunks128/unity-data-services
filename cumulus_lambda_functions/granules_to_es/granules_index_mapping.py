class GranulesIndexMapping:
    stac_mappings = {
        "event_time": {"type": "long"},
        "type": {"type": "keyword"},
        "stac_version": {"type": "keyword"},
        "id": {"type": "keyword"},
        "collection": {"type": "keyword"},
        "geometry": {"type": "geo_shape"},
        "bbox": {"type": "geo_shape"},
        "links": {
            "type": "object",
            "properties": {
                "href": {"type": "keyword"},
                "rel": {"type": "keyword"},
                "type": {"type": "keyword"},
                "title": {"type": "text"}
            }
        },
        "stac_extensions": {"type": "object"},
        "properties": {
            "dynamic": "false",
            "properties": {
                "provider": {"type": "keyword"},
                "status": {"type": "keyword"},
                "datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},
                "updated": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},
                "start_datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},
                "end_datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},
                "created": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},
            },
        },
        "assets": {
            "type": "object",
            "dynamic": False,
        }
    }
    mappings = {
        "eventTime": {"type": "long"},
        "collectionId": {"type": "keyword"},
        "createdAt": {"type": "long"},
        "duration": {"type": "float"},
        "error": {
            "type": "object",
            "properties": {
                "Cause": {"type": "text"},
                "Error": {"type": "text"}
            }
        },
        "execution": {"type": "text"},
        "granuleId": {"type": "keyword"},
        "processingEndDateTime": {"type": "date"},
        "processingStartDateTime": {"type": "date"},
        "productVolume": {"type": "keyword"},
        "provider": {"type": "keyword"},
        "published": {"type": "boolean"},
        "status": {"type": "keyword"},
        "timestamp": {"type": "long"},
        "timeToArchive": {"type": "float"},
        "timeToPreprocess": {"type": "float"},
        "updatedAt": {"type": "long"},
        "files": {
            "type": "object",
            "properties": {
                "bucket": {"type": "keyword"},
                "checksum": {"type": "keyword"},
                "checksumType": {"type": "keyword"},
                "fileName": {"type": "keyword"},
                "key": {"type": "keyword"},
                "size": {"type": "integer"},
                "source": {"type": "text"},
                "type":{"type": "keyword"}
            }
        },
        "beginningDateTime": {"type": "date"},
        "endingDateTime": {"type": "date"},
        "lastUpdateDateTime": {"type": "date"},
        "productionDateTime": {"type": "date"},
    }
