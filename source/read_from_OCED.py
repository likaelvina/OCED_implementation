import json
import urllib.parse
from rdflib import RDF, URIRef, Graph
import xml.etree.ElementTree as ET
from dateutil.parser import parse

def convert_OCED_to_rdf(oced_file_path, descriptors_file_path):

    # Load OCED file
    with open(oced_file_path, 'r') as json_file:
        # Read the JSON data from the file
        oced_model = json.load(json_file)

    # Load descriptors file
    with open(descriptors_file_path, 'r') as json_file:
        # Read the JSON data from the file
        descriptors = json.load(json_file)

    file_name = descriptors["general_information_related_to_event_log"]["file_name"]
    IRI = descriptors["general_information_related_to_event_log"]["IRI"]

    # Create an RDF graph
    g = Graph()

    # Define namespaces
    ont_ns = f"{IRI}/ontology#"
    owl_ns = "http://www.w3.org/2002/07/owl#"
    rdf_ns = "http://www.w3.org/2000/01/rdf-schema#"
    g.bind("ont", ont_ns)

    # Define classes
    events_uri = URIRef(ont_ns + "events")
    g.add((events_uri, RDF.type, URIRef(owl_ns + "Class")))

    event_type_uri = URIRef(ont_ns + "event_type")
    g.add((event_type_uri, RDF.type, URIRef(owl_ns + "Class")))

    event_timestamp_uri = URIRef(ont_ns + "event_timestamp")
    g.add((event_timestamp_uri, RDF.type, URIRef(owl_ns + "Class")))

    event_attribute_name_uri = URIRef(ont_ns + "event_attribute_name")
    g.add((event_attribute_name_uri, RDF.type, URIRef(owl_ns + "Class")))

    event_attribute_value_uri = URIRef(ont_ns + "event_attribute_value")
    g.add((event_attribute_value_uri, RDF.type, URIRef(owl_ns + "Class")))

    objects_uri = URIRef(ont_ns + "objects")
    g.add((objects_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_type_uri = URIRef(ont_ns + "object_type")
    g.add((object_type_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_attribute_name_uri = URIRef(ont_ns + "object_attribute_name")
    g.add((object_attribute_name_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_attribute_value_uri = URIRef(ont_ns + "object_attribute_value")
    g.add((object_attribute_value_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_relation_uri = URIRef(ont_ns + "object_relation")
    g.add((object_relation_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_relation_type_uri = URIRef(ont_ns + "object_relation_type")
    g.add((object_relation_type_uri, RDF.type, URIRef(owl_ns + "Class")))

    involves_object_uri = URIRef(ont_ns + "relation_involves_object")
    g.add((involves_object_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    has_attribute_name_uri = URIRef(ont_ns + "has_attribute_name")
    g.add((has_attribute_name_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    has_attribute_value_uri = URIRef(ont_ns + "has_attribute_value")
    g.add((has_attribute_value_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    has_event_type_uri = URIRef(ont_ns + "has_event_type")
    g.add((has_event_type_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    has_timestamp_uri = URIRef(ont_ns + "has_timestamp")
    g.add((has_timestamp_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    has_object_type_uri = URIRef(ont_ns + "has_object_type")
    g.add((has_object_type_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    has_position_uri = URIRef(ont_ns + "has_position")
    g.add((has_position_uri, RDF.type, URIRef(rdf_ns + "DatatypeProperty")))


    # Getting data from the OCED_Model

    # EVENT TIME instances
    key = "event_time"
    timestamps = oced_model[key]

    for timestamp in timestamps:
        date_object = parse(timestamp["event_time"])
        iso8601_timestamp = date_object.isoformat()
        event_timestamp_instance_uri = URIRef(ont_ns + iso8601_timestamp)
        g.add((event_timestamp_instance_uri, RDF.type, event_timestamp_uri))


    # EVENT TYPE instances
    key = "event_type"
    event_types = oced_model[key]

    for event_type in event_types:
        event_type_instance_uri = URIRef(ont_ns + urllib.parse.quote(event_type["event_type"]))
        g.add((event_type_instance_uri, RDF.type, event_type_uri))


    # EVENT ATTRIBUTE NAMES instances
    key = "event_attribute_name"
    event_attribute_names = oced_model[key]

    for event_attribute_name in event_attribute_names:
        event_attribute_name_instance_uri = URIRef(ont_ns + event_attribute_name["event_attribute_name"])
        g.add((event_attribute_name_instance_uri, RDF.type, event_attribute_name_uri))


    # EVENTS instances
    key = "event"
    event_dict = oced_model[key]

    for event in event_dict:
        event_instance_uri = URIRef(ont_ns + event["event_id"])
        g.add((event_instance_uri, RDF.type, events_uri))

        g.add((event_instance_uri, has_event_type_uri, URIRef(ont_ns + urllib.parse.quote(event["event_type"]))))

        date_object = parse(event["event_time"])
        iso8601_timestamp = date_object.isoformat()
        g.add((event_instance_uri, has_timestamp_uri, URIRef(ont_ns + iso8601_timestamp)))


    # EVENT ATTRIBUTE VALUES instances  &&  connection to event
    key = "event_attribute_value"
    event_attribute_values = oced_model[key]

    for event_attribute_value in event_attribute_values:
        value_full_uri = ont_ns + urllib.parse.quote(str(event_attribute_value["event_attribute_value"]))
        name_full_uri = ont_ns + urllib.parse.quote(str(event_attribute_value["event_attribute_name"]))
        g.add((URIRef(value_full_uri), RDF.type, event_attribute_value_uri))
        g.add((URIRef(value_full_uri), has_attribute_name_uri, URIRef(name_full_uri)))

        g.add((URIRef(ont_ns + event_attribute_value["event_id"]), has_attribute_value_uri, URIRef(value_full_uri)))


    # EVENT TYPE instances
    key = "object_type"
    object_types = oced_model[key]

    for object_type in object_types:
        object_type_instance_uri = URIRef(ont_ns + object_type["object_type"])
        g.add((object_type_instance_uri, RDF.type, object_type_uri))


    # OBJECT ATTRIBUTE NAMES instances
    key = "object_attribute_name"
    object_attribute_names = oced_model[key]

    for object_attribute_name in object_attribute_names:
        object_attribute_name_instance_uri = URIRef(ont_ns + object_attribute_name["object_attribute_name"])
        g.add((object_attribute_name_instance_uri, RDF.type, object_attribute_name_uri))


    # OBJECT RELATION TYPES instances 
    key = "object_relation_type"
    object_relation_types = oced_model[key]

    for object_relation_type in object_relation_types:
        object_relation_type_instance_uri = URIRef(ont_ns + object_relation_type["object_relation_type"])
        g.add((object_relation_type_instance_uri, RDF.type, object_relation_type_uri))


    # OBJECTS instances
    key = "object"
    objects = oced_model[key]

    for _object in objects:
        object_instance_uri = URIRef(ont_ns + _object["object_id"])
        g.add((object_instance_uri, RDF.type, objects_uri))
        g.add((object_instance_uri, has_object_type_uri, URIRef(ont_ns + urllib.parse.quote(_object["object_type"]))))


    # OBJECT ATTRIBUTE VALUES instances
    key = "object_attribute_value"
    object_attribute_values = oced_model[key]

    for object_attribute_value in object_attribute_values:
        value_full_uri = ont_ns + urllib.parse.quote(str(object_attribute_value["object_attribute_value"]))
        name_full_uri = ont_ns + urllib.parse.quote(str(object_attribute_value["object_attribute_name"]))
        g.add((URIRef(value_full_uri), RDF.type, object_attribute_value_uri))
        g.add((URIRef(value_full_uri), has_attribute_name_uri, URIRef(name_full_uri)))

        g.add((URIRef(ont_ns + object_attribute_value["object_id"]), has_attribute_value_uri, URIRef(value_full_uri)))


    # OBJECTS instances
    key = "object_relation"
    object_relations = oced_model[key]

    for object_relation in object_relations:
        object_relation_type_instance_uri = URIRef(ont_ns + object_relation["object_relation_type"])
        object_relation_from_instance_uri = URIRef(ont_ns + object_relation["from_object_id"])
        object_relation_to_instance_uri = URIRef(ont_ns + object_relation["to_object_id"])

        g.add((object_relation_type_instance_uri, involves_object_uri, object_relation_from_instance_uri))
        g.add((object_relation_type_instance_uri, involves_object_uri, object_relation_to_instance_uri))


    # Create an ElementTree from the root
    rdf_xml = g.serialize(format="xml")

    rdf_file_path = f"generated_documents/OCED_{file_name}_to_rdf.rdf"
    with open(rdf_file_path, "w", encoding="utf-8") as file:
        file.write(rdf_xml)
    print(f"RDF/XML content saved to {rdf_file_path}")

    rdf_data = g.serialize(format="turtle")
    # Serialize the graph to a file
    with open(f"generated_documents/OCED_{file_name}_to_owl.owl", "w", encoding="utf-8") as f:
        f.write(rdf_data)

    print(f"OWL content saved to OCED_{file_name}_to_owl.owl")
 