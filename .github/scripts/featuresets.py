import itertools
import json

FEATURES = ["gcp", "aws"]

combinations: list[dict] = []

for i in range(len(FEATURES) + 1):
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
