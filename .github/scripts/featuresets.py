import itertools
import json

FEATURES = ["gcp"]

combinations: list[dict] = []

for i in range(len(FEATURES) + 1):
    for combo in itertools.combinations(FEATURES, i):
        features = ",".join(combo)
        suffix = ""
        base = "native"

        if combo:
            suffix = "-" + "-".join(combo)

        combinations.append(
            {
                "features": features,
                "base": base,
                "suffix": suffix,
            }
        )

print(json.dumps(combinations))
