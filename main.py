from argparse import ArgumentParser
import argparse
import itertools
import multiprocessing

import requests
import pandas as pd


OBLIGATORY_FIELDS = ["number_of_connected_peers", "number_of_connected_sync_nodes", "latest_block_height", "latest_cumulative_weight", "number_of_candidate_peers"]


def fetch_data(address):
    try:
        response = requests.get(
            "http://" + address, json={"jsonrpc": "2.0", "id":"documentation", "method": "getnodestate", "params": []},
            timeout=5,
        )
        if response.status_code != 200:
            return None
        response = response.json().get("result", {})
        if not all(f in response for f in OBLIGATORY_FIELDS):
            return None
        response["connections"] = [candidate.split(":")[0] + ":3032"
                                   for candidate in itertools.chain(response.pop("candidate_peers"),
                                                                    response.pop("connected_peers"))]
        response["ip"] = address
        return response
    except Exception as e:
        return None


def generate_table(entrypoint):
    data = pd.DataFrame(columns=["ip"] + OBLIGATORY_FIELDS)
    to_query = [entrypoint]
    seen = set([entrypoint])
    while to_query:
        with multiprocessing.Pool(500) as pool:
            print("Fetching ", len(to_query), " calls in parallel...")
            result = pool.imap_unordered(fetch_data, to_query)
            to_query = []
            for item in result:
                if not item:
                    continue
                data = data.append({f: item[f] for f in OBLIGATORY_FIELDS + ["ip"]}, ignore_index=True)
                for connection in item["connections"]:
                    if connection not in seen:
                        to_query.append(connection)
                        seen.add(connection)
    return data
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry-point", default="157.90.130.43:3032")
    opts = parser.parse_args()

    data = generate_table(opts.entry_point)
    print(data)
    data.to_csv("data.csv")

