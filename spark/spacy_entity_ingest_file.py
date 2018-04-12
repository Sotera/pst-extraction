#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import re
import argparse
import json
import datetime
import spacy
from filters import valid_json_filter
from functools import partial
from pyspark import SparkContext, SparkConf

def extract_entities(doc_iter, extract_field='body', extracted_lang_field='body_lang',
                     extracted_translated_field="body_translated"):
    sys.path.append(".")

    def entities(extracted_text, lang):
        blacklist = ['subject']
        nlp = spacy.load(lang, max_length=10**7)
        extracted_text = re.sub(r'[^\x00-\x7F]', ' ', extracted_text)
        document = nlp(extracted_text)
        print 'ENTITIES ====================================='
        print document.ents
        entity_doc = {}
        # entity_doc["entity_full"] = results
        entity_doc["entity_all"] = []
        entity_doc["entity_location"] = []
        entity_doc["entity_organization"] = []
        entity_doc["entity_person"] = []
        entity_doc["entity_misc"] = []

        for ent in document.ents:
            entity_doc["entity_all"].append(ent.text)
            tag = ent.label_
            lower_text = ent.text.lower()
            if len(re.sub(r"[^\w]", '', ent.text)) <= 2 or any(item in lower_text for item in blacklist):
                continue
            if tag == 'LOC' or tag == 'GPE' or tag == 'FACILITY':
                entity_doc["entity_location"].append(ent.text)
            elif tag == 'ORG':
                entity_doc["entity_organization"].append(ent.text)
            elif tag == 'PERSON':
                entity_doc["entity_person"].append(ent.text)
            else:
                entity_doc["entity_misc"].append(ent.text)
        return entity_doc

    for doc in doc_iter:
        doc_id = doc["id"]
        if extract_field in doc:
            lang = doc.get(extracted_lang_field, 'en')

            # TODO Hack to ensure at least en is run
            # lang = lang if lang in ner_models else 'en'
            lang = 'en'

            mitie_entities = entities(doc[extract_field], lang)
            doc["entities"] = {extract_field + "_entities": mitie_entities}
            doc["entities"]["original_lang"] = lang
            doc["entities"][extract_field + "_entities_translated"] = {}

            #     Now extract entities for any translated fields
            if not lang == 'en':
                mitie_entities = entities(doc[extracted_translated_field], 'en')
                doc["entities"][extract_field + "_entities_translated"] = mitie_entities

        # TODO do attachments here instead of in a seperate execution of this stage
        yield doc


def dump(x):
    return json.dumps(x)


if __name__ == "__main__":
    print 'EXTRACTING ENTITIES!!======================================'

    desc = 'Run MITIE to generate entities for body and add them to json.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)

    parser.add_argument("input_emails_content_path", help="email json")
    parser.add_argument("output_emails_with_entities",
                        help="output directory for spark results emails with entity fields")
    parser.add_argument("--extract_field", default="body", help="Set field for MITIE to run entity extraction on.")
    parser.add_argument("-v", "--validate_json", action="store_true",
                        help="Filter broken json.  Test each json object and output broken objects to tmp/failed.")

    args = parser.parse_args()

    lex_date = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    print "INFO: Running with json filter {}.".format("enabled" if args.validate_json else "disabled")
    filter_fn = partial(valid_json_filter, os.path.basename(__file__), lex_date, not args.validate_json)

    conf = SparkConf().setAppName("Newman generate entities for emails")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_emails_content_path).filter(filter_fn).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: extract_entities(docs, args.extract_field)).map(dump).saveAsTextFile(
        args.output_emails_with_entities)