import itertools
import json
import sys

FEATURES = ["gcp", "aws"]

combinations: list[dict] = []

# With --minimal, only build the combination enabling every feature.
start = len(FEATURES) if "--minimal" in sys.argv[1:] else 0

for i in range(start, len(FEATURES) + 1):
    for combo in itertools.combinations(FEATURES, i):
        features = ",".join(combo)
        suffix = ""

        if combo:
            suffix = "-" + "-".join(combo)

        combinations.append(
            {
                "features": features,
                "suffix": suffix,
            }
        )

print(json.dumps(combinations))
