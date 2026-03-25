# Version

GET http://{{baseUrl}}/-/version
HTTP 200

[Asserts]
jsonpath "$.motiva" isString
jsonpath "$.index" isString

# Algorithms listing

GET http://{{baseUrl}}/algorithms
HTTP 200

[Asserts]
jsonpath "$.algorithms" isCollection
jsonpath "$.algorithms[*].name" includes "logic-v1"
jsonpath "$.algorithms[*].name" includes "name-based"
jsonpath "$.algorithms[*].name" includes "name-qualified"
jsonpath "$.best" == "logic-v1"
jsonpath "$.default" == "logic-v1"

# Health check

GET http://{{baseUrl}}/healthz
HTTP 200

# Readiness check

GET http://{{baseUrl}}/readyz
HTTP 200

# Catalog

GET http://{{baseUrl}}/catalog
HTTP 200

[Asserts]
jsonpath "$.datasets" count > 0
jsonpath "$.datasets[*].name" contains "us_sanctions"
