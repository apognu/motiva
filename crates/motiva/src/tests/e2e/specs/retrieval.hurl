# Basic entity retrieval

GET http://{{baseUrl}}/entities/Q7747

HTTP 200

[Asserts]
jsonpath "$.id" == "Q7747"
jsonpath "$.properties.ownershipOwner" count > 0

# Request with a referent ID

GET http://{{baseUrl}}/entities/ua-ws-person-880

HTTP 308

[Asserts]
header "location" == "/entities/Q7747"

# Entity retrieval without nested entities

GET http://{{baseUrl}}/entities/Q7747?nested=false

HTTP 200

[Asserts]
jsonpath "$.id" == "Q7747"
jsonpath "$.properties.ownershipOwner" not exists

# Non-existent entity

GET http://{{baseUrl}}/entities/DOESNOTEXIST

HTTP 404
