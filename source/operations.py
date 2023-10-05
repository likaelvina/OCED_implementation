from rdflib import RDF, URIRef, Literal
import urllib.parse

def create_classes(g, ont_ns, owl_ns, xsd_ns, descriptors):
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

    event_relation_type_uri = URIRef(ont_ns + "event_relation_type")
    g.add((event_relation_type_uri, RDF.type, URIRef(owl_ns + "Class")))

    event_related_to_uri = URIRef(ont_ns + "event_related_to")
    g.add((event_related_to_uri, RDF.type, URIRef(owl_ns + "Class")))

    objects_uri = URIRef(ont_ns + "objects")
    g.add((objects_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_type_uri = URIRef(ont_ns + "object_type")
    g.add((object_type_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_timestamp_uri = ""
    descriptors = descriptors.items()
    for key, value in descriptors:
        if key == "objects" and hasattr(value, "object_timestamp"):
            object_timestamp_uri = URIRef(ont_ns + "object_timestamp")
            g.add((object_timestamp_uri, RDF.type, URIRef(owl_ns + "Class")))
            return

    object_attribute_name_uri = URIRef(ont_ns + "object_attribute_name")
    g.add((object_attribute_name_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_attribute_value_uri = URIRef(ont_ns + "object_attribute_value")
    g.add((object_attribute_value_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_relation_type_uri = URIRef(ont_ns + "object_relation_type")
    g.add((object_relation_type_uri, RDF.type, URIRef(owl_ns + "Class")))

    object_related_to_uri = URIRef(ont_ns + "object_related_to")
    g.add((object_related_to_uri, RDF.type, URIRef(owl_ns + "Class")))

    for key, value in descriptors:
        if key == "events": 
            for attribute in value["attributes"]:
                event_attribute_name_instance_uri = URIRef(ont_ns + attribute["event_attribute_name"])
                g.add((event_attribute_name_instance_uri, RDF.type, event_attribute_name_uri))

            for relation in value["relations"]:
                event_relation_type_instance_uri = URIRef(ont_ns + relation["event_relation_type"])
                g.add((event_relation_type_instance_uri, RDF.type, event_relation_type_uri))

                event_related_to_instance_uri = URIRef(ont_ns + relation["event_related_to"])
                g.add((event_related_to_instance_uri, RDF.type, event_related_to_uri))

        if key == "objects": 
            for obj in value: 
                for attribute in obj["attributes"]:
                    object_attribute_name_instance_uri = URIRef(ont_ns + attribute["object_attribute_name"])
                    g.add((object_attribute_name_instance_uri, RDF.type, object_attribute_name_uri))

                for relation in obj["relations"]:
                    object_relation_type_instance_uri = URIRef(ont_ns + relation["object_relation_type"])
                    g.add((object_relation_type_instance_uri, RDF.type, object_relation_type_uri))

                    object_related_to_instance_uri = URIRef(ont_ns + relation["object_related_to"])
                    g.add((object_related_to_instance_uri, RDF.type, object_related_to_uri))

    has_attribute_name_uri = URIRef(ont_ns + "has_attribute_name")
    g.add((has_attribute_name_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))

    has_attribute_value_uri = URIRef(ont_ns + "has_attribute_value")
    g.add((has_attribute_value_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))

    is_part_of_event_uri = URIRef(ont_ns + "is_part_of_event")
    g.add((is_part_of_event_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))

    has_event_type_uri = URIRef(ont_ns + "has_event_type")
    g.add((has_event_type_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))

    has_timestamp_uri = URIRef(ont_ns + "has_timestamp")
    g.add((has_timestamp_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))
    
    return has_timestamp_uri, has_event_type_uri, is_part_of_event_uri, has_attribute_name_uri,has_attribute_value_uri, events_uri, event_type_uri, event_timestamp_uri, event_attribute_name_uri, event_attribute_value_uri, event_relation_type_uri, event_related_to_uri, objects_uri, object_type_uri, object_timestamp_uri, object_attribute_name_uri, object_attribute_value_uri, object_relation_type_uri, object_related_to_uri

def create_events_instances(has_timestamp_uri, has_event_type_uri, is_part_of_event_uri, has_attribute_value_uri, g, ont_ns, xsd_ns, value, row, object_type_to_URI, index,  events_uri, event_type_uri, event_timestamp_uri, event_attribute_name_uri, event_attribute_value_uri, event_relation_type_uri, event_related_to_uri, has_attribute_name_uri):
    # Event instances
    # event_instance_uri = URIRef(ont_ns + "_".join(["_".join(str(row[key]).split(" ")) for key in value["keys"] if key in row]) + "_" + str(index + 1))
    # g.add((event_instance_uri, RDF.type, events_uri))

    event_instance_uri = URIRef(ont_ns + "EventID_" + str(index + 1))
    g.add((event_instance_uri, RDF.type, events_uri))

    event_type_instance_uri = URIRef(ont_ns + "_".join(["_".join(str(row[key]).split(" ")) for key in value["keys"] if key in row]))
    g.add((event_type_instance_uri, RDF.type, event_type_uri))

    g.add((event_instance_uri, has_event_type_uri, event_type_instance_uri))

    iso8601_timestamp = row[value["event_timestamp"]].isoformat()
    event_timestamp_instance_uri = URIRef(ont_ns + iso8601_timestamp)
    g.add((event_timestamp_instance_uri, RDF.type, event_timestamp_uri))

    g.add((event_instance_uri, has_timestamp_uri, event_timestamp_instance_uri))

    object_type_to_URI["events"] = event_instance_uri

    for attribute in value["attributes"]:
        # Combine the base URI and the encoded resource name to create the full URI
        full_uri = ont_ns + urllib.parse.quote(row[attribute["event_attribute_value"]])
        g.add((URIRef(full_uri), RDF.type, event_attribute_value_uri))

        # TODO: Decide if we want to make every event_attr_value unique by adding a unique index to it
        # Or we want event_attr_value to be like it is, and this way it can connect to more than 1 event
        # Or if we want to add the values in the event instance 
        g.add((event_instance_uri, has_attribute_value_uri, URIRef(full_uri)))
        g.add((URIRef(full_uri), has_attribute_name_uri, URIRef(ont_ns + attribute["event_attribute_name"])))
        g.add((URIRef(full_uri), is_part_of_event_uri, event_instance_uri))
    
    # for relation in value["relations"]:
    #     full_uri = ont_ns + urllib.parse.quote(row[attribute["event_attribute_value"]])
    #     g.add((URIRef(ont_ns), RDF.type, event_attribute_value_uri))


    return event_instance_uri, object_type_to_URI

def create_objects_instances(g, ont_ns, xsd_ns, value, row, objects_uri, object_type_to_URI, object_type_uri, descriptors, dataframe, index, event_instance_uri, trace_instance_uri):
    for i, obj in enumerate(value):
        # Object instance
        object_instance_uri = URIRef(ont_ns + obj["object_type"] + "_" + "_".join([str(row[key]) for key in obj["keys"] if key in row]) + "_" + str(index + 1) + "_" + str(i + 1))
        g.add((object_instance_uri, RDF.type, objects_uri))

        object_type_to_URI[obj["object_type"]] = object_instance_uri

        # object_type, object_timestamp
        g.add((object_instance_uri, object_type_uri, Literal(obj["object_type"], datatype=URIRef(xsd_ns + "string"))))
        g.add((object_instance_uri, URIRef(ont_ns + obj["object_type"] + "_timestamp"), Literal(row[obj["object_timestamp"]], datatype=URIRef(xsd_ns + "dateTime"))))

        # Object attributes
        for attribute in obj["attributes"]:
            if attribute["object_attribute_value"] in dataframe.columns:
                g.add((object_instance_uri, URIRef(ont_ns + attribute["object_attribute_name"]), Literal(row[attribute["object_attribute_value"]], datatype=URIRef(xsd_ns + attribute["datatype"]))))
        
        # Event relations here because we have 1 event and more than 1 object
        # And here we have track of exact object that is connected to the above event instance
        events_relations = descriptors["events"]["relations"]
        trace_data = descriptors["trace"]
        for event_relation in events_relations:
            if event_relation["related_to"] == obj["object_type"]:
                g.add((event_instance_uri, URIRef(ont_ns + event_relation["relation_type"]), object_instance_uri ))
            if event_relation["related_to"] == trace_data["object_type"]:
                g.add((event_instance_uri, URIRef(ont_ns + event_relation["relation_type"]), trace_instance_uri ))

    # We need each object to be created first and then add the relation they might have even with each other
    for i, obj in enumerate(value):
        for relation in obj["relations"]:
            g.add((object_type_to_URI[obj["object_type"]], URIRef(ont_ns + relation["relation_type"]), object_type_to_URI[relation["related_to"]] ))

    return