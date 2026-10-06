#!/usr/bin/env python
"""
Query a list of EVA/DGVA webservice endpoints and record what they return, then optionally compare two such
recordings (e.g. the public production site vs. the new Kubernetes deployment).

This script only uses the Python standard library so that it can be run as-is from a plain python container
inside the Kubernetes cluster (see run_in_cluster.sh).

Examples:
    # Query every URL against a single base URL (e.g. the public site, from your laptop)
    python check_endpoints.py query --urls urls.txt --base-url https://www.ebi.ac.uk --output public.json

    # Query from inside the cluster, where each webservice is a separate kubernetes service
    python check_endpoints.py query --urls urls.txt --output k8s.json \
        --service /eva/webservices/rest=http://eva-ws.eva.svc.cluster.local:8080 \
        --service /eva/webservices/identifiers=http://eva-accession-ws.eva.svc.cluster.local:8080

    # Compare the two recordings
    python check_endpoints.py compare public.json k8s.json
"""
import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

# Maximum number of characters of each response body shown when printing a difference
PREVIEW_LENGTH = 300


def read_credentials(credentials_file):
    """
    Return a dict of credential name to "user:password" from the "NAME=USER:PASSWORD" lines of <credentials_file>.
    This file must stay out of version control (see .gitignore).
    """
    credentials = {}
    if not credentials_file or not os.path.exists(credentials_file):
        return credentials
    with open(credentials_file) as open_file:
        for line in open_file:
            line = line.rstrip('\n')
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            name, separator, user_password = line.partition('=')
            if not separator or ':' not in user_password:
                raise ValueError(f'Invalid line in {credentials_file}: expected NAME=USER:PASSWORD')
            credentials[name.strip()] = user_password
    return credentials


def read_requests(urls_file, credentials, credentials_file):
    """
    Return a list of (method, path, body, auth) tuples from the non-empty, non-comment lines of <urls_file>. Each
    line is "METHOD PATH [BODY [AUTH]]" where BODY is "-" or absent when there is no body and AUTH is the name of
    an entry in <credentials>, resolved here to "user:password".
    """
    requests = []
    with open(urls_file) as open_file:
        for line in open_file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            fields = line.split()
            if not 2 <= len(fields) <= 4 or not fields[1].startswith('/'):
                raise ValueError(f'Invalid line in {urls_file}: "{line}". Expected METHOD PATH [BODY [AUTH]] '
                                 f'where BODY contains no whitespace')
            body = fields[2] if len(fields) > 2 and fields[2] != '-' else None
            auth = None
            if len(fields) == 4:
                # Refuse anything that looks like actual credentials so they don't end up in version control
                if ':' in fields[3]:
                    raise ValueError(f'The AUTH column in {urls_file} must be a credential name, not user:password. '
                                     f'Put the credentials in {credentials_file} instead.')
                if fields[3] not in credentials:
                    raise ValueError(f'Credential "{fields[3]}" used in {urls_file} is not defined in '
                                     f'{credentials_file}')
                auth = credentials[fields[3]]
            requests.append((fields[0].upper(), fields[1], body, auth))
    return requests


def parse_service_mappings(service_args):
    """
    Turn a list of "PATH_PREFIX=BASE_URL" strings into a list of (prefix, base_url) tuples sorted with the longest
    prefix first so that the most specific mapping wins.
    """
    mappings = []
    for service_arg in service_args or []:
        if '=' not in service_arg:
            raise ValueError(f'Invalid --service value "{service_arg}": expected PATH_PREFIX=BASE_URL')
        prefix, base_url = service_arg.split('=', 1)
        mappings.append((prefix.rstrip('/'), base_url.rstrip('/')))
    return sorted(mappings, key=lambda mapping: len(mapping[0]), reverse=True)


def resolve_url(path, service_mappings, base_url, strip_prefix):
    """
    Build the full URL for <path> using the first service mapping whose prefix matches it, falling back to
    <base_url>. When <strip_prefix> is set, the matched prefix is removed from the path (for services that are not
    deployed under their public context path).
    """
    for prefix, service_base_url in service_mappings:
        if path == prefix or path.startswith(prefix + '/') or path.startswith(prefix + '?'):
            return service_base_url + (path[len(prefix):] if strip_prefix else path)
    if base_url:
        return base_url.rstrip('/') + path
    raise ValueError(f'No --service mapping matches {path} and no --base-url was provided')


def query_url(method, url, request_body, auth, timeout):
    """
    Send a <method> request to <url>, with <request_body> sent as JSON and <auth> ("user:password") as basic auth
    if provided, and return a dict describing the response (or the error).
    """
    # No Accept header: some endpoints only produce text/plain or application/hal+json and reply 406 otherwise
    headers = {}
    data = None
    if request_body is not None:
        headers['Content-Type'] = 'application/json'
        data = request_body.encode('utf-8')
    if auth is not None:
        headers['Authorization'] = 'Basic ' + base64.b64encode(auth.encode('utf-8')).decode('ascii')
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    start = time.time()
    # The credentials themselves are not recorded so that the output file can be shared
    result = {'method': method, 'url': url, 'request_body': request_body, 'authenticated': auth is not None,
              'status': None, 'content_type': None, 'body': None, 'error': None}
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result['status'] = response.status
            result['content_type'] = response.headers.get('Content-Type')
            result['body'] = response.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as http_error:
        # 4xx/5xx still have a body worth recording
        result['status'] = http_error.code
        result['content_type'] = http_error.headers.get('Content-Type')
        result['body'] = http_error.read().decode('utf-8', errors='replace')
    except Exception as error:
        result['error'] = f'{type(error).__name__}: {error}'
    result['elapsed_seconds'] = round(time.time() - start, 3)
    return result


def check_result(result):
    """
    Return a list of problems found in a single response: non-2xx status, empty body (only for GET, as PUT
    commonly returns 204 No Content) or invalid JSON.
    """
    if result['error']:
        return [result['error']]
    problems = []
    if not 200 <= result['status'] < 300:
        problems.append(f'HTTP {result["status"]}')
    if not result['body'] or not result['body'].strip():
        if result['method'] == 'GET':
            problems.append('empty body')
    elif 'json' in (result['content_type'] or ''):
        try:
            json.loads(result['body'])
        except ValueError:
            problems.append('invalid JSON')
    return problems


def run_queries(args):
    credentials = read_credentials(args.credentials)
    requests = read_requests(args.urls, credentials, args.credentials)
    service_mappings = parse_service_mappings(args.service)
    # Keyed by "METHOD PATH" so that the same path can be tested with several verbs
    results = {}
    nb_failed = 0
    for method, path, request_body, auth in requests:
        url = resolve_url(path, service_mappings, args.base_url, args.strip_prefix)
        result = query_url(method, url, request_body, auth, args.timeout)
        result['problems'] = check_result(result)
        results[f'{method} {path}'] = result
        if result['problems']:
            nb_failed += 1
        status = 'FAIL' if result['problems'] else 'OK'
        size = len(result['body']) if result['body'] is not None else 0
        print(f'{status:4} {str(result["status"]):4} {size:>10}B {result["elapsed_seconds"]:>7}s  {method:6} {path}'
              + (f'  -> {", ".join(result["problems"])}' if result['problems'] else ''))

    if args.output:
        with open(args.output, 'w') as open_file:
            json.dump(results, open_file, indent=2)
        print(f'Responses written to {args.output}')
    print(f'{len(requests) - nb_failed}/{len(requests)} endpoints returned a valid response')
    return 1 if nb_failed else 0


def parse_body(result):
    """Return the body parsed as JSON when possible, otherwise the raw text."""
    try:
        return json.loads(result['body'])
    except (TypeError, ValueError):
        return result['body']

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', required=True)

    query_parser = subparsers.add_parser('query', help='Query each URL and record the responses')
    query_parser.add_argument('--urls', required=True, help='File containing one path per line')
    query_parser.add_argument('--base-url', help='Base URL used for paths not matched by any --service mapping')
    query_parser.add_argument('--service', action='append', metavar='PATH_PREFIX=BASE_URL',
                              help='Send paths starting with PATH_PREFIX to BASE_URL. Can be repeated.')
    query_parser.add_argument('--strip-prefix', action='store_true',
                              help='Remove the matched PATH_PREFIX from the path before sending it to the service')
    query_parser.add_argument('--timeout', type=float, default=120, help='Timeout in seconds for each request')
    query_parser.add_argument('--output', help='JSON file where the full responses are written')
    query_parser.add_argument('--credentials', default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                   'credentials.txt'),
                              help='File of NAME=USER:PASSWORD lines referenced by name in the AUTH column of the '
                                   'URLs file (default: credentials.txt next to this script)')
    query_parser.set_defaults(func=run_queries)

    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
