import sys
import json
import requests
from pprint import pprint


def resolve_oid(node):
    for key in node.keys():
        if type(node[key]) is dict:
            if node[key].get('$oid') is None:
                resolve_oid(node[key])
            else:
                # Assuming that dict with $oid contains only it
                # let's just remove the dict with $oid with its value
                _id = node[key]['$oid']
                node[key] = _id


def extract_ids(json_string):
    json_obj = json.loads(json_string)
    #resolve_oid(json_obj)
    #_id = json_obj['_id']

    # Sometimes, _id is null [don't know why]
    if json_obj.get('_id') is None:
        return None, None

    _id = json_obj['_id']['$oid']
    json_obj.pop('_id')
    extracted = json.dumps(json_obj)
    return _id, extracted


def import_to_es(index_name, json_file):

    headers = {'content-type': 'application/json'}
    url = 'http://ochaws.com/es/' + index_name  + '/_doc/_bulk?pretty'
    print('importing %s' % index_name)

    bulk_data = {}
    index_setting_str = '{"index":{"_id":"%s"}}'

    # boot documents are crossing ES's HARDCODED limit of 2kb =(
    # https://github.com/elastic/elasticsearch/blob/master/server/src/main/java/org/elasticsearch/action/index/IndexRequest.java#L80
    # I need to do something that will cut the fild "warnings" into a separate one
    
    # if it's boot_regression, we need to increase max_fields number
    if index_name == 'boot_regressions':
        index_setting_str = '{"index":{"_id":"%s","mapping":{"total_fields":{"limit":2000}}}}'

    for line in json_file.readlines():
        _id, document = extract_ids(line)
        if _id and document:
            bulk_data[_id] = '\n'.join([index_setting_str % _id, document])

    data = '\n'.join(bulk_data.values()) + '\n'

    with open('%s.json.bulk' % (index_name), 'w') as bulk_file:
        bulk_file.write(data)

    print(data)
    sys.exit(0)

    response = requests.post(url, data=data, headers=headers)
    #print(response.text)
    try:
        response_obj = json.loads(response.text)
    except json.decoder.JSONDecodeError as err:
        print("%s: can't process server's return: %s" % (index_name, response.text))
        return
    
    errors = []
    error_str = "%s: failed to import '%s' due to '%s': %s"

    # Got a bigger error
    if response_obj.get('error'):
        error = response_obj.get('error')
        errors.append(index_name + ': ' + error['type'] + ': ' + error['reason'])  

    elif response_obj.get('errors'):
        for error in response_obj['items']:
            item = error['index']
            if item.get('error') is not None:
                errors.append(error_str % (index_name, item['_id'], item['error']['type'], item['error']['reason']))
                errors.append(bulk_data[item['_id']])

    if len(errors):
        print("%s: Some or all documents could not be imported" % (index_name), file=sys.stderr)
        print('\n'.join(errors), file=sys.stderr)
    else:
        print("%s: all documents were imported successfully" % (index_name))


def main():
    json_file_name = sys.argv[1]

    with open(json_file_name, 'r') as json_file:
        import_to_es(json_file_name.split('.')[0], json_file)

if __name__ == '__main__':
    main()
