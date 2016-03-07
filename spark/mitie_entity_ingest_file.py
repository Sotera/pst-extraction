#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import argparse
import json
from functools import partial
from pyspark import SparkContext, SparkConf


def extract_entities(doc_iter, extract_field='body', extracted_lang_field='body_lang', extracted_translated_field="body_translated"):
    sys.path.append(".")
    from mitie import tokenize_with_offsets, named_entity_extractor
    print "loading NER models..."

    ner_models={}
    ner_models['en'] = named_entity_extractor('ner_model_english.dat')
    ner_models['es'] = named_entity_extractor('ner_model_spanish.dat')

    def entities(extracted_text, lang):
            extracted_text = re.sub(r'[^\x00-\x7F]',' ', extracted_text)
            extracted_text = extracted_text.replace("[:newline:]", "           ")
            extracted_text = extracted_text.encode("ascii")
            #tokens = tokenize(body)
            tokens = tokenize_with_offsets(extracted_text)

            entities_markup = ner_models[lang].extract_entities(tokens)
            #results contains [(tag, entity, offset, score)]
            results = [
                (tag, " ".join([tokens[i][0] for i in rng]), ",".join([str(tokens[i][1]) for i in rng]), score)
                for rng, tag, score in entities_markup ]

            entity_doc = {}
            entity_doc["entity_content"] = results
            entity_doc["entity_all"] = []
            entity_doc["entity_location"] = []
            entity_doc["entity_organization"] = []
            entity_doc["entity_person"] = []
            entity_doc["entity_misc"] = []

            for tag, entity, rng, score in results:
                entity_doc["entity_all"].append(entity)

                if tag == 'LOCATION' and score > 0.3:
                    entity_doc["entity_location"].append(entity)
                elif tag == 'ORGANIZATION' and score > 0.5:
                    entity_doc["entity_organization"].append(entity)
                elif tag == 'PERSON' and score > 0.3:
                    entity_doc["entity_person"].append(entity)
                elif score > 0.5:
                    entity_doc["entity_misc"].append(entity)
            return entity_doc

    for doc in doc_iter:
        doc_id = doc["id"]
        if extract_field in doc:
            lang = doc.get(extracted_lang_field, 'en')

            # TODO Hack to ensure at least en is run
            lang = lang if lang in ner_models else 'en'

            mitie_entities = entities(doc[extract_field], lang)
            doc["entities"] = {extract_field+"_contents" : mitie_entities}
            doc["entities"]["original_lang"] = lang

        #     Now extract entities for any translated fields
            if not lang == 'en':
                mitie_entities = entities(doc[extracted_translated_field], 'en')
                doc["entities"] = {extract_field+"_contents_translated" : mitie_entities}

        # TODO do attachments here instead of in a seperate execution of this stage
        yield doc

def dump(x):
    return json.dumps(x)


if __name__ == "__main__":
    desc='Run MITIE to generate entities for body and add them to json.'
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=desc)
    
    parser.add_argument("input_emails_content_path", help="email json")
    parser.add_argument("output_emails_with_entities", help="output directory for spark results emails with entity fields")
    parser.add_argument("--extract_field", default="body", help="Set field for MITIE to run entity extraction on.")

    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman generate entities for emails")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_emails_content_path).coalesce(50).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(lambda docs: extract_entities(docs, args.extract_field)).map(dump).saveAsTextFile(args.output_emails_with_entities)

