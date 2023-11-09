class GranulesIndexMapping:
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
