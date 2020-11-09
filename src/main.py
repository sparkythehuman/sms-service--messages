import boto3
import os
from boto3.dynamodb.conditions import And, Attr, Key
from uuid import uuid4
from datetime import datetime
from pytz import timezone


message = boto3.resource('dynamodb').Table('message')

def _get_username(context):
    return context['authorizer']['claims']['cognito:username']


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
        'scheduled_messages': scheduled_messages
    }


def _get(event, context):
    scheduled_messages = _find(
        username=_get_username(context)
    )

    return _response(scheduled_messages)


def _create(event, context):
    message.put_item(Item={
        'id': 'SMS' + str(uuid4().int)[0:16],
        'contact_list_id': event['contact-list-id'],
        'send_at': event['send-at'], # TODO: convert to America/Denver if in other TZ
        'timezone': event['timezone'],
        'message': event['message'],
        'status': 'queued',
        'created_at': datetime.now(tz=timezone('America/Denver')).isoformat(),
        'updated_at': datetime.now(tz=timezone('America/Denver')).isoformat()
    })
        
    return _get(event, context) 


def handle(event, context):
    operation = context['httpMethod']
    operations = {
        'GET' : _get,
        'POST': _create
    }
    if operation in operations:
        return operations[operation](event, context)
    else:
        raise ValueError(f'Unable to run operation for HTTP METHOD: {operation}')
    