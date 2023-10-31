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

    referred = descriptors["referred_infomation_from_event_log"]
    injected = descriptors["injected_information_to_OCED_model"]

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

    # NOTE1 : Maybe for the moment I can create one property "Event involves object" that specifies all relations of event to object
    event_relation_type_uri = URIRef(ont_ns + "event_involves_object")
    g.add((event_relation_type_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

    # Getting data from the OCED_Model
    key = "time"
    timestamps = oced_model[key]

    for timestamp in timestamps:
        date_object = parse(timestamp)
        iso8601_timestamp = date_object.isoformat()
        event_timestamp_instance_uri = URIRef(ont_ns + iso8601_timestamp)
        g.add((event_timestamp_instance_uri, RDF.type, event_timestamp_uri))


    key = "event_attribute_name"
    event_attribute_names = oced_model[key]

    for event_attribute_name in event_attribute_names:
        event_attribute_name_instance_uri = URIRef(ont_ns + event_attribute_name)
        g.add((event_attribute_name_instance_uri, RDF.type, event_attribute_name_uri))


    key = "event_attribute_value"
    event_attribute_values = oced_model[key]

    for event_attribute_value in event_attribute_values.values():
        value_full_uri = ont_ns + urllib.parse.quote(str(event_attribute_value["value"]))
        name_full_uri = ont_ns + urllib.parse.quote(str(event_attribute_value["name"]))
        g.add((URIRef(value_full_uri), RDF.type, event_attribute_value_uri))
        g.add((URIRef(value_full_uri), has_attribute_name_uri, URIRef(name_full_uri)))


    key = "object_attribute_name"
    object_attribute_names = oced_model[key]

    for object_attribute_name in object_attribute_names:
        object_attribute_name_instance_uri = URIRef(ont_ns + object_attribute_name)
        g.add((object_attribute_name_instance_uri, RDF.type, object_attribute_name_uri))


    key = "object_attribute_value"
    object_attribute_values = oced_model[key]

    for object_attribute_value in object_attribute_values.values():
        value_full_uri = ont_ns + urllib.parse.quote(str(object_attribute_value["value"]))
        name_full_uri = ont_ns + urllib.parse.quote(str(object_attribute_value["name"]))
        g.add((URIRef(value_full_uri), RDF.type, event_attribute_value_uri))
        g.add((URIRef(value_full_uri), has_attribute_name_uri, URIRef(name_full_uri)))


    key = "object_relation_type"
    object_relation_types = oced_model[key]

    for object_relation_type in object_relation_types:
        object_relation_type_instance_uri = URIRef(ont_ns + object_relation_type)
        g.add((object_relation_type_instance_uri, RDF.type, object_relation_type_uri))


    key = "event_dict"
    event_dict = oced_model[key]

    for event_id, event in event_dict.items():
        event_instance_uri = URIRef(ont_ns + "EventID_" + event_id)
        g.add((event_instance_uri, RDF.type, events_uri))

        event_type_instance_uri = URIRef(ont_ns + urllib.parse.quote(event["event_type"]))
        g.add((event_type_instance_uri, RDF.type, event_type_uri))

        g.add((event_instance_uri, has_event_type_uri, event_type_instance_uri))

        date_object = parse(event["time"])
        iso8601_timestamp = date_object.isoformat()
        event_timestamp_instance_uri = URIRef(ont_ns + iso8601_timestamp)
        g.add((event_instance_uri, has_timestamp_uri, event_timestamp_instance_uri))

        for event_attribute_name, event_attribute_value in event["events_attributes"].items():
            name_full_uri = URIRef(ont_ns + urllib.parse.quote(event_attribute_name))
            value_full_uri = URIRef(ont_ns + urllib.parse.quote(event_attribute_value))
            g.add((event_instance_uri, has_attribute_value_uri, value_full_uri))


    
    # # Define a function to recursively extract information
    # for index, row in dataframe.iterrows():
    #     traceValue = "_".join(["_".join(str(row[key]).split(" ")) for key in traceKey if key in row])
    #     if not traceValue in allTraces:
    #         allTraces.append(traceValue)
    #         traceId += 1
    #         eventId = 1
    #     else: 
    #         eventId += 1

    #     key = "events"
    #     value = referred[key]       
    #     event_instance_uri = URIRef(ont_ns + "EventID_" + str(index + 1))
    #     g.add((event_instance_uri, RDF.type, events_uri))

    #     event_type_instance_uri = URIRef(ont_ns + "_".join(["_".join(str(row[key]).split(" ")) for key in value["event_type_selector"] if key in row]))
    #     g.add((event_type_instance_uri, RDF.type, event_type_uri))

    #     g.add((event_instance_uri, has_event_type_uri, event_type_instance_uri))

    #     iso8601_timestamp = row[value["event_timestamp"]].isoformat()
    #     event_timestamp_instance_uri = URIRef(ont_ns + iso8601_timestamp)
    #     g.add((event_timestamp_instance_uri, RDF.type, event_timestamp_uri))

    #     g.add((event_instance_uri, has_timestamp_uri, event_timestamp_instance_uri))

    #     for attribute in value["attributes"]:
    #         # Combine the base URI and the encoded resource name to create the full URI
    #         full_uri = ont_ns + urllib.parse.quote(str(row[attribute["event_attribute_value_selector"]]))
    #         g.add((URIRef(full_uri), RDF.type, event_attribute_value_uri))
    #         g.add((URIRef(full_uri), has_attribute_name_uri, URIRef(ont_ns + attribute["event_attribute_name"])))

    #         g.add((event_instance_uri, has_attribute_value_uri, URIRef(full_uri)))
    #         g.add((URIRef(full_uri), has_position_uri, URIRef(ont_ns + f"Trace:{traceId}/Event:{eventId}/Attribute:{attributes_positions[attribute['event_attribute_value_selector']]}")))

    #     g.add((event_instance_uri, has_position_uri, URIRef(ont_ns + f"Trace:{traceId}/Event:{eventId}")))
            
    #     key = "objects"
    #     value = referred[key]  
    #     all_event_objects = {}
    #     object_ids = {}
    #     for i, obj in enumerate(value):          
    #         object_id = "OBJ_" + "_".join(["_".join(str(row[key]).split(" ")) for key in obj["object_identifier_selector"] if key in row])
    #         object_instance_uri = URIRef(ont_ns + object_id)
    #         g.add((object_instance_uri, RDF.type, objects_uri))

    #         object_type_instance_uri = URIRef(ont_ns + obj["object_type"])
    #         g.add((object_type_instance_uri, RDF.type, object_type_uri))

    #         g.add((object_instance_uri, has_object_type_uri, object_type_instance_uri))

    #         all_event_objects[obj["object_type"]] = object_instance_uri
    #         object_ids[obj["object_type"]] = object_id

    #         for attribute in obj["attributes"]:
    #             # Combine the base URI and the encoded resource name to create the full URI
    #             full_uri = ont_ns + urllib.parse.quote(str(row[attribute["object_attribute_value_selector"]]))
    #             g.add((URIRef(full_uri), RDF.type, object_attribute_value_uri))
    #             g.add((URIRef(full_uri), has_attribute_name_uri, URIRef(ont_ns + attribute["object_attribute_name"])))
    #             g.add((object_instance_uri, has_attribute_value_uri, URIRef(full_uri)))
    #             g.add((URIRef(full_uri), has_position_uri, URIRef(ont_ns + f"Trace:{traceId}/Event:{eventId}/Attribute:{attributes_positions[attribute['object_attribute_value_selector']]}")))

    #     if "objects_relation" in injected:
    #         for rel, rel_val in injected["objects_relation"].items():
    #             if "relations" in rel_val:
    #                 for relation in rel_val["relations"]:
    #                     current_object_instance_uri = URIRef(ont_ns + object_ids[rel])
    #                     related_to_instance_uri = all_event_objects[relation["object_related_to"]]

    #                     relation_instance_uri = URIRef(ont_ns + f"{object_ids[rel]}_{object_ids[relation['object_related_to']]}")
    #                     g.add((relation_instance_uri, RDF.type, URIRef(ont_ns + f"{relation['object_relation_type']}")))

    #                     g.add((relation_instance_uri, involves_object_uri, current_object_instance_uri))
    #                     g.add((relation_instance_uri, involves_object_uri, related_to_instance_uri))

    #     # Now add the Event Relations because now all Object Instances are created and we can have the proper connections
    #     for relation in referred["events"]["relations_to_objects"]:
    #         g.add((event_instance_uri, URIRef(ont_ns + relation["event_relation_type"]), all_event_objects[f"{relation['event_related_to']}"]))

    # Create an ElementTree from the root
    rdf_xml = g.serialize(format="xml")

    rdf_file_path = f"generated_documents/{file_name}_OCED_to_rdf.rdf"
    with open(rdf_file_path, "w", encoding="utf-8") as file:
        file.write(rdf_xml)
    print(f"RDF/XML content saved to {rdf_file_path}")

    rdf_data = g.serialize(format="turtle")
    # Serialize the graph to a file
    with open(f"generated_documents/{file_name}_OCED_to_owl.owl", "w", encoding="utf-8") as f:
        f.write(rdf_data)

    print(f"OWL content saved to {file_name}_data_to_owl_xpath.owl")
 