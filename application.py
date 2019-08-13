import bs4 as bs
import urllib.request
import boto3
import pandas as pd


def get_data():
    marketplace_url = "http://modul-loadb-hpuzt26xqpk9-796477781.us-west-2.elb.amazonaws.com/marketplace/"
    print(marketplace_url)
    source = urllib.request.urlopen(marketplace_url).read()
    soup = bs.BeautifulSoup(source, "html.parser")

    all_rows = []
    for r in soup.find_all("table")[0].find_all("tr")[1:]:
        tds = r.find_all("td")
        data = [td.text for td in tds]
        all_rows.append(data)

    all_rows = sorted(all_rows, key=lambda data: float(data[4]), reverse=True)
    df = pd.DataFrame(all_rows)
    return df


def main():
    df = get_data()
    print(df)

    print("Setting up dynamoDB client")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('service-table')
    response = table.scan()
    items = response["Items"]

    for item in items:
        endpoint = item["Endpoint"]
        service_type = item["ServiceType"]
        print("Deleting: ", endpoint, service_type)
        try:
            table.delete_item(
                Key={
                    "Endpoint": endpoint,
                    "ServiceType": service_type
                }
            )
        except Exception as e:
            print(e)
            exit()

    print("Putting items")
    reliable_endpoints = [data for data in df.values if float(data[4]) > 80]
    for reliable_endpoint in reliable_endpoints:
        endpoint = reliable_endpoint[2]
        type = reliable_endpoint[1]
        accuracy = reliable_endpoint[4]
        team = reliable_endpoint[0]
        print("Putting i tem", endpoint, type, team, accuracy)
        table.put_item(
            Item={
                'Endpoint': endpoint,
                "ServiceType": type,
                "TeamName": team,
                "Accuracy": accuracy
            }
        )


if __name__ == "__main__":
    main()
