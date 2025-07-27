import json
import boto3

# ← replace with your table name
TABLE_NAME = "DevNestedTable"  

def main():
    # 1) read your JSON file
    with open("dynamodb_more_deeply_nested.json") as f:
        batch_req = json.load(f).get(TABLE_NAME, [])

    # 2) init DynamoDB resource & table
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table(TABLE_NAME)

    # 3) batch‐writer will auto‑paginate and back‑off for you
    with table.batch_writer() as writer:
        for req in batch_req:
            item = req["PutRequest"]["Item"]
            writer.put_item(Item=item)

    print(f"Loaded {len(batch_req)} items into {TABLE_NAME}")

if __name__ == "__main__":
    main()

