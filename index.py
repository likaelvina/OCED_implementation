from source.manual_solution import convert_xes_to_rdf_manual_position
from source.xpath_solution import convert_xes_to_rdf_xpath_position
from source.read_from_OCED import convert_OCED_to_rdf

convert_xes_to_rdf_manual_position("data.xes", "descriptors.json")
convert_xes_to_rdf_xpath_position("data.xes", "descriptors.json")
convert_OCED_to_rdf("OCED_data.json", "descriptors.json")