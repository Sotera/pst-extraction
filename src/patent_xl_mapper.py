import xmltodict
import dateutil.parser

def parties(bib):
    if 'parties' in bib:
        applicants = bib['parties']['applicants']['applicant']
    else:
        applicants = bib['us-parties']['us-applicants']['us-applicant']
    _applicants = []
    i = 0
    #applicants
    if type(applicants)==type([]):
        for a in applicants:
            temp = {}
            if 'last-name' in a['addressbook']:
                temp['name'] = a['addressbook']['first-name'] + " " + a['addressbook']['last-name'] if 'first-name' in a['addressbook'] else a['addressbook']['last-name']
            else:
                temp['name'] = a['addressbook']['orgname']
            temp['country'] = a['addressbook']['address']['country'] if 'address' in a['addressbook'] else None
            _applicants.append(temp)
    else:
        if 'last-name' in applicants['addressbook']:
            temp = {'name':applicants['addressbook']['first-name'] + " " + applicants['addressbook']['last-name'] if 'first-name' in applicants['addressbook'] else applicants['addressbook']['last-name']}
            temp['country'] = applicants['addressbook']['address']['country'] if 'address' in applicants['addressbook'] else None
            _applicants.append(temp)
        else:
            temp = {'name':applicants['addressbook'][u'orgname']}
            temp['country'] = applicants['addressbook']['address']['country'] if 'address' in applicants['addressbook'] else None
            _applicants.append(temp)
    #inventors
    if 'us-parties' in bib and 'inventors' in bib['us-parties']:
        inv = bib['us-parties']['inventors']['inventor']
        if type(inv) == type([]):
            for a in inv:
                temp = {'name':a['addressbook']['first-name'] + " " + a['addressbook']['last-name'] if 'first-name' in a['addressbook'] else a['addressbook']['last-name']}
                temp['country'] = a['addressbook']['address']['country'] if 'address' in a['addressbook'] else None
                _applicants.append(temp)
        else:
            temp = {'name':inv['addressbook']['first-name'] + " " + inv['addressbook']['last-name'] if 'first-name' in inv['addressbook'] else inv['addressbook']['last-name']}
            temp['country'] = {inv['addressbook']['address']['country'] if 'address' in inv['addressbook'] else None}
            _applicants.append(temp)

    return _applicants

def rec_to_row(line):
    try:
        tree = xmltodict.parse(line)
    except:
        return None
    if 'us-patent-application' not in tree:
        return None
    bib = tree['us-patent-application']['us-bibliographic-data-application']
    pub_ref = bib['publication-reference']['document-id']
    _bib_pub_data = pub_ref['country'] + "_" + pub_ref['doc-number'] + "_" + pub_ref['date']
    _id = pub_ref['doc-number']
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

    _applicants = parties(bib)
    try:
        _abstract = tree['us-patent-application']['abstract']['p']['#text']
    except:
        _abstract = ''
        for p in tree['us-patent-application']['abstract']['p']:
            if '#text' in p:
                _abstract = _abstract + " " + p['#text']

    return {"pub_data": _bib_pub_data,
                "id": _id,
                "applicants": _applicants,
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
