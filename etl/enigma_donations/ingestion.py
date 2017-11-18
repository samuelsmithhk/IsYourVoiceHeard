import argparse
import requests


def main():
    ap = argparse.ArgumentParser("Download csv from Enigma")
    ap.add_argument('enigma_key', help="The API key to use for downloading from Enigma")
    ap.add_argument('--output', '-o', help="The output file (default=output.csv)", default='output.csv')

    args = ap.parse_args()

    headers = {
        "Authorization": "Bearer " + args.enigma_key
    }

    csv_response = requests.get("https://public.enigma.com/api/export/97a6153c-213e-46a6-acd0-ad36f0f3aff0",
                                headers=headers, stream=True)


    with open(args.output, 'wb') as file_out:
        # File can be large, so only keep 1mb in memory at a time
        for chunk in csv_response.iter_content(chunk_size=1024):
            if chunk:
                file_out.write(chunk)
                print("Saved a mb")

    print("Done")


if __name__ == "__main__":
    main()
