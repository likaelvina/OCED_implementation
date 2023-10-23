import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import json

class sub_event:
    '''
    Class to handle sub_events

    Attributes
    ----------
    function : OCED.qualifier_function
        Function to execute
    parameters : dict
        Function parameters
    
    Methods
    -------
    None

    Notes
    -----
    None

    Examples
    --------
    None
    '''
    def __init__(self, function_name, parameters):
        '''
        Parameters
        ----------
        function_name : str
            Function name
        parameters : dict
            Function parameters
        
        Returns
        -------
        None

        Raises
        ------
        TypeError
            If function_name is not a string
            If parameters is not a dictionary of string : string
        ValueError
            If function_name is not a valid function name
            If parameters does not contain all the parameters of the function
        
        Notes
        -----
        None

        Examples
        --------
        None
        '''
        functions = {
            'create_object': {
                'function': OCED.create_object,
                'parameters': ['object_id', 'object_type']
            },
            'create_object_relation': {
                'function': OCED.create_object_relation,
                'parameters': ['object_relation_id', 'from_object_id', 'to_object_id', 'relation_type']
            },
            'create_object_attribute_value': {
                'function': OCED.create_object_attribute_value,
                'parameters': ['object_attribute_value_id', 'object_id', 'name', 'value']
            },
            'delete_object': {
                'function': OCED.delete_object,
                'parameters': ['object_id']
            },
            'delete_object_relation': {
                'function': OCED.delete_object_relation,
                'parameters': ['object_relation_id']
            },
            'delete_object_attribute_value': {
                'function': OCED.delete_object_attribute_value,
                'parameters': ['object_attribute_value_id']
            },
            'modify_object': {
                'function': OCED.modify_object,
                'parameters': ['object_id', 'new_object_type']
            },
            'modify_object_relation': {
                'function': OCED.modify_object_relation,
                'parameters': ['object_relation_id', 'new_relation_type']
            },
            'modify_object_attribute_value': {
                'function': OCED.modify_object_attribute_value,
                'parameters': ['object_attribute_value_id', 'new_value']
            }
        }
        # check function_name
        if not isinstance(function_name, str):
            raise TypeError('function_name must be a string')
        if function_name not in functions.keys():
            raise ValueError('function_name must be a valid function name')
        # check parameters
        if not isinstance(parameters, dict):
            raise TypeError('parameters must be a dictionary of string : string')
        for parameter in functions[function_name]['parameters']:
            if parameter not in parameters.keys():
                raise ValueError('parameters must contain all the parameters of the function')
            if not isinstance(parameters[function_name][parameter], str):
                raise TypeError('parameters must be a dictionary of string : string')
        # assign variables
        self.function = functions[function_name]['function']
        self.parameters = parameters
        return

class event:
    '''
    Class to handle events

    Attributes
    ----------
    time : str ISO 8601-1:2019
        Event time
    event_type : str
        Event type
    sub_events : list
        List of sub_events
    object_ids_involved : list
        List of object ids involved
    object_relation_ids_involved : list
        List of object relation ids involved
    object_attribute_ids_involved : list
        List of object attribute ids involved
    events_attributes : dict
        Dictionary of event attributes
    
    Methods
    -------
    None

    Notes
    -----
    None

    Examples
    --------
    None
    '''
    def __init__(self, time, event_type, events_attributes={}, sub_events=[], object_ids_involved=[], object_relation_ids_involved=[], object_attribute_value_ids_involved=[]):
        '''
        Parameters
        ----------
        time : str ISO 8601-1:2019
            Event time
        event_type : str
            Event type
        events_attributes : dict of str : str
            Event attributes
        sub_events : list
            List of sub_events
        object_ids_involved : list
            List of object ids involved
        object_relation_ids_involved : list
            List of object relation ids involved
        object_attribute_value_ids_involved : list
            List of object attribute value ids involved
        
        Returns
        -------
        None

        Raises
        ------
        TypeError
            If time is not a string ISO 8601-1:2019
            If event_type is not a string
            If events_attributes is not a dictionary of string : string
            If sub_events is not a list of sub_event
            If object_ids_involved is not a list of string
            If object_relation_ids_involved is not a list of string
            If object_attribute_value_ids_involved is not a list of string
        '''
        # check time
        try:
            time = datetime.fromtimestamp(datetime.fromisoformat(time).timestamp()).isoformat()
        except:
            raise TypeError('time must be a valid ISO 8601-1:2019 string')
        # check event_id
        if not isinstance(event_type, str):
            raise TypeError('event_type must be a string')
        # check event_attributes
        if not isinstance(events_attributes, dict):
            raise TypeError('events_attributes must be a dictionary of string : string')
        for name, value in events_attributes.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("events_attributes must be a dictionary of string : string")
        # check sub_events
        if not isinstance(sub_events, list):
            raise TypeError('sub_events must be a list of sub_event')
        for elem in sub_events:
            if not isinstance(elem, sub_event):
                raise TypeError("sub_events must be a list of sub_event")
        # check object_ids_involved
        if not isinstance(object_ids_involved, list):
            raise TypeError('object_ids_involved must be a list of string')
        object_ids_involved = set(object_ids_involved)
        for elem in object_ids_involved:
            if not isinstance(elem, str):
                raise TypeError("object_ids_involved must be a list of string")
        # check object_relation_ids_involved
        if not isinstance(object_relation_ids_involved, list):
            raise TypeError('object_relation_ids_involved must be a list of string')
        object_relation_ids_involved = set(object_relation_ids_involved)
        for elem in object_relation_ids_involved:
            if not isinstance(elem, str):
                raise TypeError("object_relation_ids_involved must be a list of string")
        # check object_attribute_value_ids_involved
        if not isinstance(object_attribute_value_ids_involved, list):
            raise TypeError('object_attribute_value_ids_involved must be a list of string')
        object_attribute_value_ids_involved = set(object_attribute_value_ids_involved)
        for elem in object_attribute_value_ids_involved:
            if not isinstance(elem, str):
                raise TypeError("object_attribute_value_ids_involved must be a list of string")
        # assign variables
        self.time = time
        self.event_type = event_type
        self.sub_events = sub_events
        self.object_ids_involved = object_ids_involved
        self.object_relation_ids_involved = object_relation_ids_involved
        self.object_attribute_value_ids_involved = object_attribute_value_ids_involved
        self.events_attributes = events_attributes
        return

class OCED:
    '''
    Object-Centric Event Data

    Attributes
    ----------
    event : pandas.DataFrame
        DataFrame with events
    event_type : pandas.DataFrame
        DataFrame with event types
    time : pandas.DataFrame
        DataFrame with times
    event_attribute_name : pandas.DataFrame
        DataFrame with event attribute names
    event_attribute_value : pandas.DataFrame
        DataFrame with event attribute values
    object : pandas.DataFrame
        DataFrame with objects
    object_type : pandas.DataFrame
        DataFrame with object types
    object_attribute_name : pandas.DataFrame
        DataFrame with object attribute names
    object_attribute_value : pandas.DataFrame
        DataFrame with object attribute values
    object_relation : pandas.DataFrame
        DataFrame with object relations
    object_relation_type : pandas.DataFrame
        DataFrame with object relation types
    event_x_object : pandas.DataFrame
        DataFrame with events x objects
    events_x_object_attribute_value : pandas.DataFrame
        DataFrame with events x object attribute values
    events_x_object_relation : pandas.DataFrame
        DataFrame with events x object relations
    event_dict : dict
        Dictionary with events
    object_dict : dict
        Dictionary with objects
    object_attribute_value_dict : dict
        Dictionary with object attribute values
    object_relation_dict : dict
        Dictionary with object relations
    log : list
        List with operations

    Methods
    -------
    insert_event(event)
        Insert an event
    revert_event(event)
        Revert an event
    create_object(object_id, object_type)
        Create an object
    create_object_relation(object_relation_id, from_object_id, to_object_id, relation_type)
        Create an object relation
    create_object_attribute_value(object_attribute_value_id, object_id, name, value)
        Create an object attribute value
    delete_object(object_id)
        Delete an object
    delete_object_relation(object_relation_id)
        Delete an object relation
    delete_object_attribute_value(object_attribute_value_id)
        Delete an object attribute value
    modify_object(object_id, new_object_type)
        Modify an object
    modify_object_relation(object_relation_id, new_relation_type)
        Modify an object relation
    modify_object_attribute_value(object_attribute_value_id, new_value)
        Modify an object attribute value

    Notes
    -----
    None

    Examples
    --------
    None
    '''
    def __init__(self):
        '''
        Parameters
        ----------
        None
        
        Returns
        -------
        None

        Raises
        ------
        None

        Notes
        -----
        None

        Examples
        --------
        None
        '''
        self.event = pd.DataFrame(columns=['id'])
        # event
        # | event_id | time | event_type |
        # primary key: event_id
        # foreign key: event_type references event_type.name
        # foreign key: time references time.value
        self.event_type = pd.DataFrame(columns=['name'])
        # event_type
        # | name |
        # primary key: name
        self.time = pd.DataFrame(columns=['value'])
        # time
        # | value |
        # primary key: value
        self.event_attribute_name = pd.DataFrame(columns=['name'])
        # event_attribute_name
        # | name |
        # primary key: name
        self.event_attribute_value = pd.DataFrame(columns=['event_id', 'name', 'value'])
        # event_attribute_value
        # | event_id | name | value |
        # primary key: (event_id, name)
        # foreign key: event_id references event.event_id
        # foreign key: name references event_attribute_name.name
        self.object = pd.DataFrame(columns=['object_id', 'object_type'])
        # object
        # | object_id | object_type |
        # primary key: object_id
        # foreign key: object_type references object_type.name
        self.object_type = pd.DataFrame(columns=['name'])
        # object_type
        # | name |
        # primary key: name
        self.object_attribute_name = pd.DataFrame(columns=['name'])
        # object_attribute_name
        # | name |
        # primary key: name
        self.object_attribute_value = pd.DataFrame(columns=['object_attribute_value_id', 'object_id', 'name', 'value'])
        # object_attribute_value
        # | object_attribute_value_id | object_id | name | value |
        # primary key: object_attribute_value_id
        # foreign key: object_id references object.object_id
        # foreign key: name references object_attribute_name.name
        self.object_relation = pd.DataFrame(columns=['object_relation_id', 'from_object_id', 'to_object_id', 'relation_type'])
        # object_relation
        # | object_relation_id | from_object_id | to_object_id | relation_type |
        # primary key: object_relation_id
        # foreign key: from_object_id references object.object_id
        # foreign key: to_object_id references object.object_id
        # foreign key: relation_type references object_relation_type.name
        self.object_relation_type = pd.DataFrame(columns=['name'])
        # object_relation_type
        # | name |
        # primary key: name
        self.event_x_object = pd.DataFrame(columns=['event_id', 'object_id'])
        # events_x_object
        # | event_id | object_id |
        # primary key: (event_id, object_id)
        # foreign key: event_id references event.event_id
        # foreign key: object_id references object.object_id
        self.events_x_object_attribute_value = pd.DataFrame(columns=['event_id', 'object_attribute_value_id'])
        # events_x_object_attribute_value
        # | event_id | object_attribute_value_id |
        # primary key: (event_id, object_attribute_value_id)
        # foreign key: event_id references event.event_id
        # foreign key: object_attribute_value_id references object_attribute_value.object_attribute_value_id
        self.events_x_object_relation = pd.DataFrame(columns=['event_id', 'object_relation_id'])
        # events_x_object
        # | event_id | object_id |
        # primary key: (event_id, object_relation_id)
        # foreign key: event_id references events.event_id
        # foreign key: object_id references object_relations.object_relation_id
        self.event_dict = {}
        '''
        event_dict
        {
            'event_id1': { # event_id
                'time': string object ISO 8601-1:2019,
                'event_type': string object,
                'object_ids_involved': list of string object_ids,
                'object_relation_ids_involved': list of string object_relation_ids,
                'object_attribute_value_ids_involved': list of string object_attribute_value_ids,
                'events_attributes': {
                    'name1': 'value1',
                    'name2': 'value2'
                }
            }
        }
        '''
        self.object_dict = {}
        '''
        object_dict
        {
            'object_id1': {
                'object_type': 'name',
                'attribute_value_ids': [],
                'object_relation_ids_me_to_other': [],
                'object_relation_ids_other_to_me': [],
                'involved_in_event_ids': []
            }
        }
        '''
        self.object_attribute_value_dict = {}
        '''
        object_attribute_value_dict
        {
            'object_attribute_value_id1': {
                'object_id': 'object_id1',
                'name': 'name',
                'value': 'value',
                'involved_in_event_ids': ['event_id1', 'event_id2']
            }
        }
        '''
        self.object_relation_dict = {}
        '''
        object_relation_dict
        {
            'object_relation_id1': {
                'from_object_id': 'obejct_id1',
                'to_object_id': 'object_id2',
                'relation_type': 'name',
                'involved_in_event_ids': ['event_id1', 'event_id2']
            }
        }
        '''
        self.log = []
        '''
        log
        [
            [
                {
                    'function': 'function_name',
                    'parameters': {
                        'parameter_name1': 'parameter_value2'
                    }
                }
            ]
        ]
        '''
    def insert_event(self, event):
        '''
        ValueError
            If after all sub_events all object_ids_involved are not in the object_ids
            If after all sub_events all object_relation_ids_involved are not object_relation_ids
            If after all sub_events all object_attribute_value_ids_involved are not value_ids
        '''
        pass
    def revert_event(self, event):
        pass
    def create_object(self, object_id, object_type):
        pass
    def create_object_relation(self, object_relation_id, from_object_id, to_object_id, relation_type):
        pass
    def create_object_attribute_value(self, object_attribute_value_id, object_id, name, value):
        pass
    def delete_object(self, object_id):
        pass
    def delete_object_relation(self, object_relation_id):
        pass
    def delete_object_attribute_value(self, object_attribute_value_id):
        pass
    def modify_object(self, object_id, new_object_type):
        pass
    def modify_object_relation(self, object_relation_id, new_relation_type):
        pass
    def modify_object_attribute_value(self, object_attribute_value_id, new_value):
        pass

def load_from_json(file_name):
    pass

def load_from_xml(file_name):
    pass

def dump_to_json(file_name, OCED):
    pass

def dump_to_xml(file_name, OCED):
    pass

'''
def add_object_relation(self, object_relation_id, from_object_id, to_object_id, relation_type):
        
    Add a new object relation

    Parameters
    ----------
    object_relation_id : str
        Object relation id
    from_object_id : str
        From object id
    to_object_id : str
        To object id
    relation_type : str
        Relation type
    
    Returns
    -------
    None

    Raises
    ------
    TypeError
        If object_relation_id is not a string
        If from_object_id is not a string
        If to_object_id is not a string
        If relation_type is not a string
    ValueError
        If object_relation_id is not a new object_relation_id
        If from_object_id is not an existing object_id
        If to_object_id is not an existing object_id

    Notes
    -----
    None

    Examples
    --------
    >>> objects_relations.add('object_relation_id1', 'object_id1', 'object_id2', 'name')
    
    # check errors
    if not isinstance(object_relation_id, str):
        raise TypeError('object_relation_id must be a string')
    if not isinstance(from_object_id, str):
        raise TypeError('from_object_id must be a string')
    if not isinstance(to_object_id, str):
        raise TypeError('to_object_id must be a string')
    if not isinstance(relation_type, str):
        raise TypeError('relation_type must be a string')
    if object_relation_id in self.dict:
        raise ValueError('object_relation_id must be a new object_relation_id')
    if from_object_id not in self.OCED.objects.dict:
        raise ValueError('from_object_id must be an existing object_id')
    if to_object_id not in self.OCED.objects.dict:
        raise ValueError('to_object_id must be an existing object_id')
    # update objects_relations.dict
    self.dict[object_relation_id] = {
        'from_object_id': from_object_id,
        'to_object_id': to_object_id,
        'relation_type': relation_type,
        'involved_in_event_ids': []
    }
    # update objects_relations.df
    row = {
        'object_relation_id': object_relation_id,
        'from_object_id': from_object_id,
        'to_object_id': to_object_id,
        'relation_type': relation_type
    }
    self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)
    # update objects.dict
    self.OCED.objects.dict[from_object_id]['object_relation_ids_me_to_other'].append(object_relation_id)
    self.OCED.objects.dict[to_object_id]['object_relation_ids_other_to_me'].append(object_relation_id)
    # update object_relation_types.df
    if relation_type not in self.OCED.object_relation_types.df['name'].values:
        row = {'name': relation_type}
        self.OCED.object_relation_types.df = pd.concat([self.OCED.object_relation_types.df, pd.DataFrame([row])], ignore_index=True)
    # add event
    self.OCED.events.add('object_relation_added', object_relation_id, [from_object_id, to_object_id], [], {'relation_type': relation_type})
'''



'''
    if not isinstance(object_ids_involved, list):
        raise TypeError('object_ids_involved must be a list of strings')
    for object_id in object_ids_involved:
        if not isinstance(object_id, str):
            raise TypeError('object_ids_involved must be a list of strings')
        if object_id not in self.OCED.objects.dict:
            raise ValueError('all object_ids_involved must be object_ids in objects')
    if not isinstance(object_relation_ids_involved, list):
        raise TypeError('object_relation_ids_involved must be a list of strings')
    for object_relation_id in object_relation_ids_involved:
        if not isinstance(object_relation_id, str):
            raise TypeError('object_relation_ids_involved must be a list of strings')
        if object_relation_id not in self.OCED.object_relations.dict:
            raise ValueError('all object_relation_ids_involved must be object_relation_ids in object_relations')
    if not isinstance(object_attribute_value_ids_involved, list):
        raise TypeError('object_attribute_value_ids_involved must be a list of strings')
    for object_attribute_value_id in object_attribute_value_ids_involved:
        if not isinstance(object_attribute_value_id, str):
            raise TypeError('object_attribute_value_ids_involved must be a list of strings')
        if object_attribute_value_id not in self.OCED.object_attribute_values.dict:
            raise ValueError('all object_attribute_value_ids_involved must be object_attribute_value_ids in object_attribute_values')
    if not isinstance(events_attributes, dict):
        raise TypeError('events_attributes must be a dictionary string: string')
    for name, value in events_attributes.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError('events_attributes must be a dictionary string: string')
    if self.OCED.times.df.shape[0] > 0 and time <= self.OCED.times.df['time'].max():
        raise ValueError('time must be the current max time')
    # update last_event_id
    self.last_event_id += 1
    event_id = str(self.last_event_id)
    # update events.dict
    self.dict[event_id] = {
        'event_type': event_type,
        'time': time,
        'object_ids_involved': object_ids_involved,
        'object_relation_ids_involved': object_relation_ids_involved,
        'object_attribute_value_ids_involved': object_attribute_value_ids_involved,
        'events_attributes': events_attributes
    }
    # update objects.dict and objects_x_events.df
    rows = {'event_id': [], 'object_id': []}
    for object_id in object_ids_involved:
        self.OCED.objects.dict[object_id]['involved_in_event_ids'].append(event_id)
        rows['event_id'].append(event_id)
        rows['object_id'].append(object_id)
    self.OCED.events_x_objects.df = pd.concat([self.OCED.events_x_objects.df, pd.DataFrame.from_dict(rows)], ignore_index=True)
    # update object_relations.dict and events_x_object_relations.df
    rows = {'event_id': [], 'object_relation_id': []}
    for object_relation_id in object_relation_ids_involved:
        self.OCED.object_relations.dict[object_relation_id]['involved_in_event_ids'].append(event_id)
        rows['event_id'].append(event_id)
        rows['object_relation_id'].append(object_relation_id)
    self.OCED.events_x_object_relations.df = pd.concat([self.OCED.events_x_object_relations.df, pd.DataFrame.from_dict(rows)], ignore_index=True)
    # update object_attribute_values.dict and events_x_object_attribute_values.df
    rows = {'event_id': [], 'object_attribute_value_id': []}
    for object_attribute_value_id in object_attribute_value_ids_involved:
        self.OCED.object_attribute_values.dict[object_attribute_value_id]['event_ids_involved'].append(event_id)
        rows['event_id'].append(event_id)
        rows['object_attribute_value_id'].append(object_attribute_value_id)
    self.OCED.events_x_object_attribute_values.df = pd.concat([self.OCED.events_x_object_attribute_values.df, pd.DataFrame.from_dict(rows)], ignore_index=True)
    # update events.df
    row = {'event_id': [event_id], 'event_type': [event_type], 'time': [time]}
    self.df = pd.concat([self.df, pd.DataFrame.from_dict(row)], ignore_index=True)
    # update event_types.df
    if event_type not in self.OCED.event_types.df['name'].values:
        row = {'name': [event_type]}
        self.OCED.event_types.df = pd.concat([self.OCED.event_types.df, pd.DataFrame.from_dict(row)], ignore_index=True)
    # update times.df
    row = {'value': [time]}
    self.OCED.times.df = pd.concat([self.OCED.times.df, pd.DataFrame.from_dict(row)], ignore_index=True)
    # update event_attribute_values.df
    rows = {'id': [], 'name': [], 'value': []}
    for name, value in events_attributes.items():
        rows['id'].append(event_id)
        rows['name'].append(name)
        rows['value'].append(value)
    self.OCED.event_attribute_values.df = pd.concat([self.OCED.event_attribute_values.df, pd.DataFrame.from_dict(rows)], ignore_index=True)
    # update event_attribute_names.df
    rows = {'name': []}
    event_attribute_names = self.OCED.event_attribute_names.df['name'].values
    for name in events_attributes.keys():
        if name not in event_attribute_names:
            rows['name'].append(name)
    self.OCED.event_attribute_names.df = pd.concat([self.OCED.event_attribute_names.df, pd.DataFrame.from_dict(rows)], ignore_index=True)
    # update log
    operation = {
        'function': 'add',
        'parameters' : {
            'event_id': event_id,
            'time': time,
            'event_type': event_type,
            'object_ids_involved': object_ids_involved,
            'object_relation_ids_involved': object_relation_ids_involved,
            'object_attribute_value_ids_involved': object_attribute_value_ids_involved,
            'events_attributes': events_attributes
        }
    }
    self.OCED.log.append(operation)
    return


'''