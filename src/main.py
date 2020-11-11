import boto3
import json
import os
from boto3.dynamodb.conditions import And, Attr, Key
from uuid import uuid4
from datetime import datetime
from pytz import timezone


message = boto3.resource('dynamodb').Table('message')


def _get_username(event):
    return event['requestContext']['authorizer']['claims']['cognito:username']


def _find(username, **kwargs):
    query_params = {
        'IndexName': 'username-index',
        'KeyConditionExpression': Key('username').eq(username),
    }
    if kwargs:
        filter_condition_expressions = [ Attr(key).eq(value) for key, value in kwargs.items() ]
        if len(filter_condition_expressions) > 1:
            query_params['FilterExpression'] = And(*filter_condition_expressions)
        elif len(filter_condition_expressions) == 1:
            query_params['FilterExpression'] = filter_condition_expressions[0]
    
    data = message.query(**query_params)

    return data['Items']


def _response(scheduled_messages):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(scheduled_messages)
    }


def _get(request, username):
    scheduled_messages = _find(
        username=username
    )

    return _response(scheduled_messages)


def _create(request, username):
    message.put_item(Item={
        'id': 'SMS' + str(uuid4().int)[0:16],
        'contact_list_id': request['contact_list_id'],
        # TODO: we may have to concat a date, time, and timezone
        'send_at': request['send_at'],
        'message': request['message'],
        'status': 'queued',
        'username': username,
        'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
        'updated_at': datetime.now(tz=timezone('America/Denver')).isoformat()
    })
        
    return _get(request, username) 


def handle(event, context):
    operation = event['requestContext']['httpMethod']
    operations = {
        'GET' : _get,
        'POST': _create
    }
    if operation in operations:
        return operations[operation](json.loads(event['body']), _get_username(event))
    else:
        raise ValueError(f'Unable to run operation for HTTP METHOD: {operation}')
    