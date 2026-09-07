# Basic match query

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich PUTIN"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" == 1.0

# Match query with normalized country

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"],
                "citizenship": ["Russia"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich PUTIN"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" == 1.0

# Match query with wrong normalized country

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"],
                "citizenship": ["France"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich PUTIN"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" == 0.8

# Match query with name parts

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Cannot Be Found"],
                "firstName": ["Vladimir"],
                "fatherName": ["Vladimirovich"],
                "lastName": ["Putin"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich PUTIN"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" == 1.0

# Match with matching datasets filter

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"]
            }
        }
    },
    "params": {
        "include_datasets": ["eu_fsf"]
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich PUTIN"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" == 1.0

# Match with non-matching datasets filter

POST http://{{baseUrl}}/match/default
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"]
            }
        }
    },
    "params": {
        "include_datasets": ["us_state_terrorist_exclusion"]
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results" count == 0

# Match query with scope partition and wrong scope

POST http://{{baseUrl}}/match/default?partition=true
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results" count == 0

# Match query with scope partition and right scope

POST http://{{baseUrl}}/match/eu_fsf?partition=true
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Vladimirovich Putin"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results" count > 0
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].caption" contains "Vladimir Vladimirovich PUTIN"
jsonpath "$.responses.test.results[?(@.id == 'Q7747')].score" == 1.0

# Instance-wide custom weight

POST http://{{baseUrl}}/match/eu_fsf?algorithm=marble-v0&threshold=0.0&cutoff=0.0
{
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Gorlov"],
                "idNumber": ["INVALID"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].caption" contains "Vladimir Aleksandrovich GORLOV"
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].score" == 0.6
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].features.identifier_mismatch" == 1.0

# Query-level custom weights

POST http://{{baseUrl}}/match/eu_fsf?algorithm=marble-v0&threshold=0.0&cutoff=0.0
{
    "weights": {
        "identifier_mismatch": -0.2
    },
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Gorlov"],
                "idNumber": ["INVALID"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].caption" contains "Vladimir Aleksandrovich GORLOV"
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].score" == 0.7
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].features.identifier_mismatch" == 1.0

# Score rounding

POST http://{{baseUrl}}/match/eu_fsf?algorithm=marble-v0&threshold=0.0&cutoff=0.0
{
    "weights": {
        "identifier_mismatch": -0.21243
    },
    "queries": {
        "test": {
            "schema": "Person",
            "properties": {
                "name": ["Vladimir Gorlov"],
                "idNumber": ["INVALID"]
            }
        }
    }
}

HTTP 200

[Asserts]
jsonpath "$.responses.test.status" == 200
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].caption" contains "Vladimir Aleksandrovich GORLOV"
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].score" == 0.688
jsonpath "$.responses.test.results[?(@.id == 'NK-3NKxS96tyPZgxZpeHJ4wJA')].features.identifier_mismatch" == 1.0
