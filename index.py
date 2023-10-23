from source.manual_solution import convert_xes_to_rdf_manual_position
from source.xpath_solution import convert_xes_to_rdf_xpath_position

convert_xes_to_rdf_manual_position("data.xes", "descriptors.json")
convert_xes_to_rdf_xpath_position("data.xes", "descriptors.json")