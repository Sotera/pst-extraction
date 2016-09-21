__author__ = 'root'

import xmltodict
import json
from datetime import datetime
import dateutil
import dateutil.parser


def rec_to_row(line):
    tree = xmltodict.parse(line)
    bib = tree['us-patent-application']['us-bibliographic-data-application']
    pub_ref = bib['publication-reference']['document-id']
    _bib_pub_data = pub_ref['country'] + "_" + pub_ref['doc-number'] + "_" + pub_ref['date']
    app_ref = bib['application-reference']['document-id']
    _bib_app_data = app_ref['country'] + "_" + app_ref['doc-number'] + "_" + app_ref['date']
    _application_series = bib['us-application-series-code']
    _title = bib['invention-title']['#text'] + " " + bib['invention-title']['@id']
    try:
        parent = bib['us-related-documents']['division']['relation']['parent-doc']['document-id']
        _parent_id = parent['country'] + "_" + parent['doc-number'] + "_" + parent['date']
    except:
        _parent_id = None

    try:
        child = bib['us-related-documents']['division']['relation']['child-doc']['document-id']
        _child_id = child['country'] + "_" + child['doc-number'] + "_" + child['date']
    except:
        _child_id = None

    try:
        prov = bib['us-related-documents']['us-provisional-application']['document-id']
        _prov = prov['country'] + "_" + prov['doc-number'] + "_" + prov['date']
    except:
        _prov = None

    try:
        applicants = bib['parties']['applicants']['applicant']
        _applicants = {}
        i = 0
        if type(applicants)==type([]):
            for a in applicants:
                _applicants[str(i)] = {}
                _applicants[str(i)]['name'] = a['addressbook']['first-name'] + " " + a['addressbook']['last-name'] if 'first-name' in a['addressbook'] else a['addressbook']['last-name']
                _applicants[str(i)]['country'] = a['addressbook']['address']['country']
                i += 1
        else:
            _applicants[str(i)] = {'name':applicants['addressbook']['first-name'] + " " + applicants['addressbook']['last-name'] if 'first-name' in applicants['addressbook'] else applicants['addressbook']['last-name'],
                                   'country':applicants['addressbook']['address']['country']}
    except:
        return None

    try:
        _abstract = tree['us-patent-application']['abstract']['p']['#text']
    except:
        _abstract = ''
        for p in tree['us-patent-application']['abstract']['p']:
            if '#text' in p:
                _abstract = _abstract + " " + p['#text']

    return {"pub_data": _bib_pub_data,
               "app_data": _bib_app_data,
               "pub_date": dateutil.parser.parse(tree['us-patent-application']['@date-publ']).strftime('%Y-%m-%dT%H:%M:%S'),
               "prod_date": dateutil.parser.parse(tree['us-patent-application']['@date-produced']).strftime('%Y-%m-%dT%H:%M:%S'),
               "series": _application_series,
               "title": _title,
               "abstract": _abstract,
               "parent": _parent_id,
               "provisional_app": _prov,
               "child": _child_id
              }

#    return Row(pub_data = _bib_pub_data,
#               app_data = _bib_app_data,
#               pub_date = datetime.strptime(tree['us-patent-application']['@date-publ'], '%Y%m%d'),
#               prod_date = datetime.strptime(tree['us-patent-application']['@date-produced'], '%Y%m%d'),
#               series = _application_series,
#               title = _title,
#               abstract = _abstract,
#               parent = _parent_id,
#               provisional_app = _prov,
#               child = _child_id
#              )
