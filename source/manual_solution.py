import json
import pm4py
import urllib.parse
from rdflib import RDF, URIRef, Graph

def convert_xes_to_rdf_manual_position(xes_file_path, descriptors_file_path):
    # Load XES file
    log = pm4py.read_xes(xes_file_path)
    # Get file data
    dataframe = pm4py.convert_to_dataframe(log)

    # Load descriptors file
    with open(descriptors_file_path, 'r') as json_file:
        # Read the JSON data from the file
        descriptors = json.load(json_file)

    # Validate JSON file so that it has the proper information
    errors = []

    if not "referred_infomation_from_event_log" in descriptors:
        errors.append("You need to have referred_infomation_from_event_log in the descriptor file!")

    if not "injected_information_to_OCED_model" in descriptors:
        errors.append("You need to have injected_information_to_OCED_model in the descriptor file!")

    if len(errors) > 0:
        print(errors)
        return
    
    referred = descriptors["referred_infomation_from_event_log"]
    injected = descriptors["injected_information_to_OCED_model"]

    if not "events" in referred:
        errors.append("You need to have event data in the descriptor file!")

    if not "objects" in referred:
        errors.append("You need to have objects data in the descriptor file!")

    if "events" in referred:
        if not isinstance(referred["events"], dict):
            errors.append("events should be an object!")
        else: 
            if not "event_type_selector" in referred["events"]:
                errors.append("You need to have event_type_selector in the descriptor file, they represent the value of Event Type!")
            if not "event_timestamp" in referred["events"]:
                errors.append("You need to have event_timestamp in the descriptor file, it represents the Event Timestamp!")
            for attr in referred["events"]["attributes"]:
                if not "event_attribute_value_selector" in attr or not "event_attribute_name" in attr:
                    errors.append("You need to have event_attribute_value_selector and event_attribute_name for each attribute in the descriptor file!")
                    break
            for relation in referred["events"]["relations_to_objects"]:
                if not "event_relation_type" in relation or not "event_related_to" in relation:
                    errors.append("You need to have event_relation_type and event_related_to for each relation in the descriptor file!")
                    break

    if "objects" in referred:
        if not isinstance(referred["objects"], list):
            errors.append("objects should be array!")
        else: 
            for obj in referred["objects"]:
                if not "object_type" in obj:
                    errors.append("You need to have object_type data in the descriptor file for each object!")
                if not "object_identifier_selector" in obj:
                    errors.append("You need to have object_identifier_selector in the descriptor file for each object!")
                for attr in obj["attributes"]:
                    if not "object_attribute_value_selector" in attr or not "object_attribute_name" in attr:
                        errors.append("You need to have object_attribute_value_selector and object_attribute_name for each attribute in the descriptor file!")
                        break
                if "relations" in obj:
                    for relation in obj["relations"]:
                        if not "object_relation_type" in relation or not "object_related_to" in relation:
                            errors.append("You need to have object_relation_type and object_related_to for each relation in the descriptor file!")
                            break

    if len(errors) > 0:
        return

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

    for key, value in referred.items():
        if key == "events": 
            for attribute in value["attributes"]:
                event_attribute_name_instance_uri = URIRef(ont_ns + attribute["event_attribute_name"])
                g.add((event_attribute_name_instance_uri, RDF.type, event_attribute_name_uri))

            for relation in value["relations_to_objects"]:
                event_relation_type_uri = URIRef(ont_ns + relation["event_relation_type"])
                g.add((event_relation_type_uri, RDF.type, URIRef(rdf_ns + "ObjectProperty")))

        if key == "objects": 
            for obj in value: 
                for attribute in obj["attributes"]:
                    object_attribute_name_instance_uri = URIRef(ont_ns + attribute["object_attribute_name"])
                    g.add((object_attribute_name_instance_uri, RDF.type, object_attribute_name_uri))

                if "relations" in obj:
                    for relation in obj["relations"]:
                        object_relation_type_instance_uri = URIRef(ont_ns + relation["object_relation_type"])
                        g.add((object_relation_type_instance_uri, RDF.type, object_relation_type_uri))


    if "objects_relation" in injected:
        for rel, rel_val in injected["objects_relation"].items():
            if "relations" in rel_val:
                for relation in rel_val["relations"]:
                    object_relation_type_instance_uri = URIRef(ont_ns + relation["object_relation_type"])
                    g.add((object_relation_type_instance_uri, RDF.type, object_relation_type_uri))

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

    allTraces = []
    matching_objects = [obj for obj in referred["objects"] if "is_trace" in obj and obj["is_trace"] is True]
    if len(matching_objects) > 0:
        traceKey = matching_objects[0]["object_identifier_selector"]
    else: 
        traceKey = ""

    traceId = 0
    eventId = 0

    attributes_positions = {}
    for index, col_name in enumerate(dataframe.columns):
        attributes_positions[col_name]= index

    # Add instances for the classes and properties based on the data file we have
    for index, row in dataframe.iterrows():
        traceValue = "_".join(["_".join(str(row[key]).split(" ")) for key in traceKey if key in row])
        if not traceValue in allTraces:
            allTraces.append(traceValue)
            traceId += 1
            eventId = 1
        else: 
            eventId += 1

        key = "events"
        value = referred[key]       
        event_instance_uri = URIRef(ont_ns + "EventID_" + str(index + 1))
        g.add((event_instance_uri, RDF.type, events_uri))

        event_type_instance_uri = URIRef(ont_ns + "_".join(["_".join(str(row[key]).split(" ")) for key in value["event_type_selector"] if key in row]))
        g.add((event_type_instance_uri, RDF.type, event_type_uri))

        g.add((event_instance_uri, has_event_type_uri, event_type_instance_uri))

        iso8601_timestamp = row[value["event_timestamp"]].isoformat()
        event_timestamp_instance_uri = URIRef(ont_ns + iso8601_timestamp)
        g.add((event_timestamp_instance_uri, RDF.type, event_timestamp_uri))

        g.add((event_instance_uri, has_timestamp_uri, event_timestamp_instance_uri))

        for attribute in value["attributes"]:
            # Combine the base URI and the encoded resource name to create the full URI
            full_uri = ont_ns + urllib.parse.quote(str(row[attribute["event_attribute_value_selector"]]))
            g.add((URIRef(full_uri), RDF.type, event_attribute_value_uri))
            g.add((URIRef(full_uri), has_attribute_name_uri, URIRef(ont_ns + attribute["event_attribute_name"])))

            g.add((event_instance_uri, has_attribute_value_uri, URIRef(full_uri)))
            g.add((URIRef(full_uri), has_position_uri, URIRef(ont_ns + f"Trace:{traceId}/Event:{eventId}/Attribute:{attributes_positions[attribute['event_attribute_value_selector']]}")))

        g.add((event_instance_uri, has_position_uri, URIRef(ont_ns + f"Trace:{traceId}/Event:{eventId}")))
            
        key = "objects"
        value = referred[key]  
        all_event_objects = {}
        object_ids = {}
        for i, obj in enumerate(value):          
            object_id = "OBJ_" + "_".join(["_".join(str(row[key]).split(" ")) for key in obj["object_identifier_selector"] if key in row])
            object_instance_uri = URIRef(ont_ns + object_id)
            g.add((object_instance_uri, RDF.type, objects_uri))

            object_type_instance_uri = URIRef(ont_ns + obj["object_type"])
            g.add((object_type_instance_uri, RDF.type, object_type_uri))

            g.add((object_instance_uri, has_object_type_uri, object_type_instance_uri))

            all_event_objects[obj["object_type"]] = object_instance_uri
            object_ids[obj["object_type"]] = object_id

            for attribute in obj["attributes"]:
                # Combine the base URI and the encoded resource name to create the full URI
                full_uri = ont_ns + urllib.parse.quote(str(row[attribute["object_attribute_value_selector"]]))
                g.add((URIRef(full_uri), RDF.type, object_attribute_value_uri))
                g.add((URIRef(full_uri), has_attribute_name_uri, URIRef(ont_ns + attribute["object_attribute_name"])))
                g.add((object_instance_uri, has_attribute_value_uri, URIRef(full_uri)))
                g.add((URIRef(full_uri), has_position_uri, URIRef(ont_ns + f"Trace:{traceId}/Event:{eventId}/Attribute:{attributes_positions[attribute['object_attribute_value_selector']]}")))

        if "objects_relation" in injected:
            for rel, rel_val in injected["objects_relation"].items():
                if "relations" in rel_val:
                    for relation in rel_val["relations"]:
                        current_object_instance_uri = URIRef(ont_ns + object_ids[rel])
                        related_to_instance_uri = all_event_objects[relation["object_related_to"]]

                        relation_instance_uri = URIRef(ont_ns + f"{object_ids[rel]}_{object_ids[relation['object_related_to']]}")
                        g.add((relation_instance_uri, RDF.type, URIRef(ont_ns + f"{relation['object_relation_type']}")))

                        g.add((relation_instance_uri, involves_object_uri, current_object_instance_uri))
                        g.add((relation_instance_uri, involves_object_uri, related_to_instance_uri))

        # Now add the Event Relations because now all Object Instances are created and we can have the proper connections
        for relation in referred["events"]["relations_to_objects"]:
            g.add((event_instance_uri, URIRef(ont_ns + relation["event_relation_type"]), all_event_objects[f"{relation['event_related_to']}"]))

    # Create an ElementTree from the root
    rdf_xml = g.serialize(format="xml")

    rdf_file_path = f"generated_documents/{file_name}_data_to_rdf.rdf"
    with open(rdf_file_path, "w", encoding="utf-8") as file:
        file.write(rdf_xml)
    print(f"RDF/XML content saved to {rdf_file_path}")

    rdf_data = g.serialize(format="turtle")
    # Serialize the graph to a file
    with open(f"generated_documents/{file_name}_data_to_owl.owl", "w", encoding="utf-8") as f:
        f.write(rdf_data)

    print(f"OWL content saved to {file_name}_data_to_owl.owl")
