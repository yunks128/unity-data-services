class GranulesIndexMapping:
    stac_mappings = {
        "eventTime": {"type": "long"},
        "type": {"type": "keyword"},
        "stac_version": {"type": "keyword"},
        "id": {"type": "keyword"},
        "collection": {"type": "keyword"},
        "properties": {
            "dynamic": "false",
            "properties": {
                "datetime": {"type": "date"},
                "updated": {"type": "date"},
                "start_datetime": {"type": "date"},
                "end_datetime": {"type": "date"},
                "created": {"type": "date"},
            },
        },
        "assets": {
            "type": "nested",
            "dynamic": "true",
            "properties": {
                "dynamic_field": {
                    "type": "nested",
                    "dynamic": "true",
                    "properties": {
                        "href": {"type": "keyword"},
                        "title": {"type": "text"},
                        "description": {"type": "text"}
                    }
                }
            }
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
