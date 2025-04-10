import boto3
import os
from lambdas.config import get_table_name

_table = None  # <-- Cached table object

def get_dynamodb_table():
    global _table
    if _table is None:
        dynamodb = boto3.resource("dynamodb")
        _table = dynamodb.Table(get_table_name())
    return _table
