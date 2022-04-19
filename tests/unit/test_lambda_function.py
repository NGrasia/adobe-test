import json
import pytest

from code import lambda_function


@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "2022-04-19T18:56:00.187Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "A3IQN195QTTWRT"
      },
      "requestParameters": {
        "sourceIPAddress": "136.37.176.94"
      },
      "responseElements": {
        "x-amz-request-id": "ZSRDS495HTTEJ77R",
        "x-amz-id-2": "9eyXMGmRAxln7VCk5ususTYetpCIYk3YSOSXOOqOG0JX3rNP2XhRr7D2looszFFfVHXKiX71hfgBS6LmLiGKq0eR6o/Ua+81mGITFt58t5Q="
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "landing-file",
        "bucket": {
          "name": "ncode-landing-bucket",
          "ownerIdentity": {
            "principalId": "A3IQN195QTTWRT"
          },
          "arn": "arn:aws:s3:::ncode-landing-bucket"
        },
        "object": {
          "key": "sample_files/data.tsv",
          "size": 6259,
          "eTag": "c5cf3ac1e540a3a98685b2787cc27059",
          "sequencer": "00625F05C029824347"
        }
      }
    }
  ]
}


def test_lambda_handler(apigw_event, mocker):

    ret = lambda_function.lambda_handler(apigw_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "Test1"
    # assert "location" in data.dict_keys()
