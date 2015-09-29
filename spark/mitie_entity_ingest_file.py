#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import argparse
import json
from functools import partial
from pyspark import SparkContext, SparkConf


#def spit(filePath, data, overwrite=False):
#    mode= 'w' if overwrite else 'a'
#    with open(filePath, mode) as x: x.write(data)

def extract_entities(doc_iter):
    sys.path.append(".")
    from mitie import tokenize_with_offsets, named_entity_extractor
    print "loading NER model..."
    ner = named_entity_extractor('ner_model.dat')

    print "\nTags output by this NER model:", ner.get_possible_ner_tags()
    for doc in doc_iter:
        doc_id = doc["id"]
        body = doc["body"]
        body = re.sub(r'[^\x00-\x7F]',' ', body)
        body = body.replace("[:newline:]", "           ")
        body = body.encode("ascii")
        #tokens = tokenize(body)
        tokens = tokenize_with_offsets(body)
        entities_markup = ner.extract_entities(tokens)
        #results contains [(tag, entity, offset, score)]
        results = [
            (tag, " ".join([tokens[i][0] for i in rng]), ",".join([str(tokens[i][1]) for i in rng]), score)
            for rng, tag, score in entities_markup ]
        
        #only want to return the entity name
        entities = [(b)for a,b,c,d in results]
        
        doc["entities"] = {}
        doc["entities"]["entity_content"] = results
        doc["entities"]["entity_all"] = entities
        doc["entities"]["entity_location"] = []
        doc["entities"]["entity_organization"] = []
        doc["entities"]["entity_person"] = []
        doc["entities"]["entity_misc"] = []
        
        for tag, entity, rng, score in results:
            if tag == 'LOCATION' and score > 0.3:
                doc["entities"]["entity_location"].append(entity)
            elif tag == 'ORGANIZATION' and score > 0.5:
                doc["entities"]["entity_organization"].append(entity)
            elif tag == 'PERSON' and score > 0.3:
                doc["entities"]["entity_person"].append(entity)
            elif score > 0.5:
                doc["entities"]["entity_misc"].append(entity)
 
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

    args = parser.parse_args()

    conf = SparkConf().setAppName("Newman generate entities for emails")
    sc = SparkContext(conf=conf)

    rdd_emails = sc.textFile(args.input_emails_content_path).map(lambda x: json.loads(x))
    rdd_emails.mapPartitions(extract_entities).map(dump).saveAsTextFile(args.output_emails_with_entities)

