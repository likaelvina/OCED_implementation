from rdflib import RDF, URIRef, Literal

def create_classes_and_properties(g, ont_ns, owl_ns, xsd_ns, descriptors):
    # Define classes
    events_uri = URIRef(ont_ns + "events")
    objects_uri = URIRef(ont_ns + "objects")

    g.add((events_uri, RDF.type, URIRef(owl_ns + "Class")))
    g.add((objects_uri, RDF.type, URIRef(owl_ns + "Class")))

    # Define datatype properties
    event_type_uri = URIRef(ont_ns + "event_type")
    g.add((event_type_uri, RDF.type, URIRef(owl_ns + "DatatypeProperty")))
    g.add((event_type_uri, URIRef(owl_ns + "domain"), URIRef(events_uri)))
    g.add((event_type_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + "string")))

    object_type_uri = URIRef(ont_ns + "object_type")
    g.add((object_type_uri, RDF.type, URIRef(owl_ns + "DatatypeProperty")))
    g.add((object_type_uri, URIRef(owl_ns + "domain"), URIRef(objects_uri)))
    g.add((object_type_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + "string")))

    # Object having all DatatypeProperty names as keys, and the exact key in xml tag as value
    names_to_xml_keys = {}

    for key, value in descriptors.items():
        if key == "events":
            names_to_xml_keys["events"] = value["keys"]
            names_to_xml_keys["event_timestamp"] = value["event_timestamp"]

            event_timestamp_uri = URIRef(ont_ns + "event_timestamp")
            g.add((event_timestamp_uri, RDF.type, URIRef(owl_ns + "DatatypeProperty")))
            g.add((event_timestamp_uri, URIRef(owl_ns + "domain"), URIRef(events_uri)))
            g.add((event_timestamp_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + "dateTime")))

            for attribute in value["attributes"]:
                names_to_xml_keys[attribute["event_attribute_name"]] = [attribute["event_attribute_value"]]

                attribute_uri = URIRef(ont_ns + attribute["event_attribute_name"])
                g.add((attribute_uri, RDF.type, URIRef(owl_ns + "DatatypeProperty")))
                g.add((attribute_uri, URIRef(owl_ns + "domain"), URIRef(events_uri)))
                g.add((attribute_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + attribute["datatype"])))

            for relation in value["relations"]:
                relation_uri = URIRef(ont_ns + relation["relation_type"])
                g.add((relation_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))
                g.add((relation_uri, URIRef(owl_ns + "domain"), URIRef(events_uri)))
                g.add((relation_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + relation["related_to"])))

        if key == "objects":
            for obj in value:
                names_to_xml_keys[obj["object_type"]] = obj["keys"]

                object_timestamp = obj["object_type"] + "_timestamp"
                names_to_xml_keys[object_timestamp] = obj["object_timestamp"]

                object_timestamp_uri = URIRef(ont_ns + object_timestamp)
                g.add((object_timestamp_uri, RDF.type, URIRef(owl_ns + "DatatypeProperty")))
                g.add((object_timestamp_uri, URIRef(owl_ns + "domain"), URIRef(objects_uri)))
                g.add((object_timestamp_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + "dateTime")))

                for attribute in obj["attributes"]:
                    names_to_xml_keys[attribute["object_attribute_name"]] = [attribute["object_attribute_value"]]

                    attribute_uri = URIRef(ont_ns + attribute["object_attribute_name"])
                    g.add((attribute_uri, RDF.type, URIRef(owl_ns + "DatatypeProperty")))
                    g.add((attribute_uri, URIRef(owl_ns + "domain"), URIRef(objects_uri)))
                    g.add((attribute_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + attribute["datatype"])))

                for relation in obj["relations"]:
                    relation_uri = URIRef(ont_ns + relation["relation_type"])
                    g.add((relation_uri, RDF.type, URIRef(owl_ns + "ObjectProperty")))
                    g.add((relation_uri, URIRef(owl_ns + "domain"), URIRef(objects_uri)))
                    g.add((relation_uri, URIRef(owl_ns + "range"), URIRef(xsd_ns + relation["related_to"])))

    return events_uri, objects_uri, event_type_uri, event_timestamp_uri, object_type_uri, names_to_xml_keys

def create_trace_instances(g, ont_ns, xsd_ns, value, row, objects_uri, object_type_to_URI, object_type_uri, dataframe, index):
    # Object instance
    trace_instance_uri = URIRef(ont_ns + value["object_type"] + "_" + "_".join([str(row[f"case:{key}"]) for key in value["keys"] if key in row]))
    g.add((trace_instance_uri, RDF.type, objects_uri))

    object_type_to_URI[value["object_type"]] = trace_instance_uri

    # object_type
    g.add((trace_instance_uri, object_type_uri, Literal(value["object_type"], datatype=URIRef(xsd_ns + "string"))))

    # Trace attributes
    for attribute in value["attributes"]:
        if f"case:{attribute['object_attribute_value']}" in dataframe.columns:
            g.add((trace_instance_uri, URIRef(ont_ns + attribute["object_attribute_name"]), Literal(row[f"case:{attribute['object_attribute_value']}"], datatype=URIRef(xsd_ns + attribute["datatype"]))))

    return trace_instance_uri, object_type_to_URI

def create_events_instances(g, ont_ns, xsd_ns, value, row, events_uri, object_type_to_URI, event_type_uri, event_timestamp_uri, names_to_xml_keys, index):
    # Event instance
    event_instance_uri = URIRef(ont_ns + "_".join(["_".join(str(row[key]).split(" ")) for key in value["keys"] if key in row]) + "_" + str(index + 1))
    g.add((event_instance_uri, RDF.type, events_uri))

    object_type_to_URI["events"] = event_instance_uri

    # event_type, event_timestamp
    g.add((event_instance_uri, event_type_uri, Literal(" ".join([row[key] for key in names_to_xml_keys["events"] if key in row]), datatype=URIRef(xsd_ns + "string"))))
    g.add((event_instance_uri, event_timestamp_uri, Literal(row[names_to_xml_keys["event_timestamp"]], datatype=URIRef(xsd_ns + "dateTime"))))

    # event attributes
    for attribute in value["attributes"]:
        g.add((event_instance_uri, URIRef(ont_ns + attribute["event_attribute_name"]), Literal(row[attribute["event_attribute_value"]], datatype=URIRef(xsd_ns + attribute["datatype"]))))

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