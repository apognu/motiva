# Basic match query

POST http://{{baseUrl}}/match/default
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
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich Putin"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" first == 1.0

# Match query with normalized country

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Putin"],
                "citizenship": ["Russia"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich Putin"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" first == 1.0

# Match query with wrong normalized country

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Putin"],
                "citizenship": ["France"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich Putin"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" first == 0.8
