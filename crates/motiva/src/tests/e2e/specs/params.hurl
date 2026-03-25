# Match with limit=1

POST http://{{baseUrl}}/match/default?limit=1
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Putin"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.limit" == 1
jsonpath "$.responses.test.results" count == 1

# Match with high threshold still returns strong match

POST http://{{baseUrl}}/match/default?threshold=0.99
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Putin"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.results" count == 1
