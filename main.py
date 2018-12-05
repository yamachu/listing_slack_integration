import argparse
import csv
import itertools
import json
import urllib.request
from operator import itemgetter

# 使うAPI
LOGS_URL = 'https://slack.com/api/team.integrationLogs'
FETCH_USER_BY_EMAIL_URL = 'https://slack.com/api/users.lookupByEmail'
GET_TEAM_DOMAIN = 'https://slack.com/api/team.info'


def fetch_integrations(token: str, user_id: str):
    param = {
        'token': token,
        'user': user_id,
    }

    def _fetch_internal(page: int):
        param['page'] = page
        req = urllib.request.Request('{}?{}'.format(
            LOGS_URL, urllib.parse.urlencode(param)))

        with urllib.request.urlopen(req) as res:
            return json.load(res)

    current_page = 1
    total_pages = 1

    result_logs = []

    while current_page <= total_pages:
        res = _fetch_internal(current_page)

        if res['ok'] is False:
            raise Exception(
                'cannot execute integrationLogs API, {}'.format(res))

        result_logs.extend(res['logs'])

        total_pages = res['paging']['pages']
        current_page += 1

    return result_logs


def fetch_user_id_from_email(token: str, email: str):
    param = {
        'token': token,
        'email': email
    }

    def _fetch_internal():
        req = urllib.request.Request('{}?{}'.format(FETCH_USER_BY_EMAIL_URL, urllib.parse.urlencode(param)))

        with urllib.request.urlopen(req) as res:
            return json.load(res)

    res = _fetch_internal()

    if res['ok'] is False:
        raise Exception('cannot execute lookupByEmail API, {}'.format(res))

    return res['user']['id']


def fetch_team_domain(token: str):
    param = {
        'token': token
    }

    def _fetch_internal():
        req = urllib.request.Request('{}?{}'.format(
            GET_TEAM_DOMAIN, urllib.parse.urlencode(param)))

        with urllib.request.urlopen(req) as res:
            return json.load(res)

    res = _fetch_internal()

    if res['ok'] is False:
        raise Exception('cannot execute team.info API, {}'.format(res))

    return res['team']['domain']


def grouping_integration_status(src):
    SERVICE_ID = 'service_id'
    APP_ID = 'app_id'

    service_id_group = filter(lambda x: SERVICE_ID in x, src)
    app_id_group = filter(lambda x: APP_ID in x, src)
    others = list(
        filter(lambda x: SERVICE_ID not in x and APP_ID not in x, src))

    def _group_by_id(group, key):
        sorted_group = sorted(group, key=itemgetter(key, 'date'))
        return dict({k: list(v) for k, v in itertools.groupby(sorted_group, key=itemgetter(key))})

    _s = _group_by_id(service_id_group, SERVICE_ID)
    _a = _group_by_id(app_id_group, APP_ID)

    return (_s, _a, others)


def generate_csv(file_name: str, groups, domain: str, summary=False):
    def _write_all(writer: csv.DictWriter):
        [writer.writerow({
            'integration_id': k,
            'change_type': v['change_type'],
            'integration_type': v['service_type'] if 'service_type' in v else v['app_type'],
            'channel': "https://{}.slack.com/messages/{}/".format(domain, v['channel']) if 'channel' in v else '',
            'date': v['date']
        }) for g in groups for k, vx in g.items() for v in vx]

    def _write_summary(writer: csv.DictWriter):
        [writer.writerow({
            'should_check': vx[-1]['change_type'] not in ['removed', 'disabled'],
            'integration_id': k,
            'integration_type': _first(vx, 'service_type') if _has(vx, 'service_type') else _first(vx, 'app_type'),
            'channel': "https://{}.slack.com/messages/{}/".format(domain, _first(vx, 'channel')) if _has(vx, 'channel') else '',
        }) for g in groups for k, vx in g.items()]

    with open(file_name, 'w') as fw:
        field = ['should_check', 'integration_id', 'integration_type', 'channel'] if summary \
            else ['integration_id', 'change_type',
                  'integration_type', 'channel', 'date']
        writer = csv.DictWriter(fw, fieldnames=field)
        writer.writeheader()

        if summary:
            _write_summary(writer)
        else:
            _write_all(writer)


def _has(arr, key: str):
    return len(list(filter(lambda x: key in x, arr))) > 0


def _first(arr, key: str):
    return next(x for x in arr if lambda x: key in x)[key]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('email', help='email of user who has integration')
    parser.add_argument('token', help='admin token that has "admin,identify,users:read,users:read.email,team:read" scope')
    parser.add_argument('save_path', help='output file path')
    parser.add_argument('--format', choices=['raw', 'csv-full', 'csv-summary'], default='csv-full')
    args = parser.parse_args()

    token = args.token
    email = args.email

    user_id = fetch_user_id_from_email(token, email)
    alls = fetch_integrations(token, user_id)
    domain = fetch_team_domain(token)

    s, a, o = grouping_integration_status(alls)

    if args.format == 'raw':
        with open(args.save_path, 'w') as f:
            json.dump(alls, f)
    else:
        generate_csv(args.save_path, [s, a], domain, args.format == 'csv-summary')

    if len(o) > 0:
        print('cannot parse following entries')
        print(o)
