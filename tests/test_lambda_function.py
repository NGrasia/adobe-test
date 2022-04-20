import os
# import boto3
# from moto import mock_s3
import pytest
import json
# from code import FileProcess

def create_sample():
    par_dir = os.path.join(os.path.dirname(__file__),"lambda_events","tsv_file.json")
    with open(par_dir,"r") as f_in:
        data = json.loads(f_in.read())
    return data

SAMPLES = create_sample()

def test_samples_exist():
    assert SAMPLES is not None
    assert isinstance(SAMPLES,list) is True
    assert len(SAMPLES) > 1

@pytest.mark.parametrize("event",SAMPLES)
def test_lambda_event(event):
    data = "true"
    assert data is not None
    print(data)