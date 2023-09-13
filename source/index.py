import json
import pm4py
from rdflib import Graph
from .operations import create_classes_and_properties, create_trace_instances, create_events_instances, create_objects_instances

def convert_xes_to_rdf(xes_file_path, descriptors_file_path):
    # Load XES file
    # log = pm4py.read_xes("bpi_challenge_2013_open_problems.xes")
    log = pm4py.read_xes(xes_file_path)
    # Get file data
    dataframe = pm4py.convert_to_dataframe(log)

    # Load descriptors file
    with open(descriptors_file_path, 'r') as json_file:
        # Read the JSON data from the file
        descriptors = json.load(json_file)

    file_name = descriptors["file_name"]
    domain = descriptors["domain"]

    # Create an RDF graph
    g = Graph()

    # Define namespaces
    ont_ns = f"http://{domain}/ontology#"
    owl_ns = "http://www.w3.org/2002/07/owl#"
    xsd_ns = "http://www.w3.org/2001/XMLSchema#"
    g.bind("ont", ont_ns)

    events_uri, objects_uri, event_type_uri, event_timestamp_uri, object_type_uri, names_to_xml_keys = create_classes_and_properties(g, ont_ns, owl_ns, xsd_ns, descriptors)

    # Save URIs of objects so we can connect later if we have relations Product to Product, Event to Product or Product to Event
    object_type_to_URI = {}

    # Add instances for the classes and properties based on the data file we have
    for index, row in dataframe.iterrows():
        for key, value in descriptors.items():
            if key == "trace": 
                trace_instance_uri, object_type_to_URI = create_trace_instances(g, ont_ns, xsd_ns, value, row, objects_uri, object_type_to_URI, object_type_uri, dataframe, index)
            if key == "events":
                event_instance_uri, object_type_to_URI = create_events_instances(g, ont_ns, xsd_ns, value, row, events_uri, object_type_to_URI, event_type_uri, event_timestamp_uri, names_to_xml_keys, index)
            if key == "objects":
                create_objects_instances(g, ont_ns, xsd_ns, value, row, objects_uri, object_type_to_URI, object_type_uri, descriptors, dataframe, index, event_instance_uri, trace_instance_uri)

    # Create an ElementTree from the root
    rdf_xml = g.serialize(format="xml")

    rdf_file_path = f"{file_name}_data_to_rdf.rdf"
    with open(rdf_file_path, "w", encoding="utf-8") as file:
        file.write(rdf_xml)
    print(f"RDF/XML content saved to {rdf_file_path}")

    rdf_data = g.serialize(format="turtle")
    # Serialize the graph to a file
    with open(f"{file_name}_data_to_owl.owl", "w", encoding="utf-8") as f:
        f.write(rdf_data)

    print(f"OWL content saved to test.owl")
