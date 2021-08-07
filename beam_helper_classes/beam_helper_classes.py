import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions, GoogleCloudOptions


__all__ = ['Output', 'SplitByDelimiter', 'FormatToDict', 'SplitToDict',  'Dtype_Transform', 'InnerJoin']


class Output(beam.PTransform, beam.DoFn):
    def process(self, element):
        yield print(element)
    
    def expand(self, pcoll):
        return (pcoll | beam.ParDo(self.process))

class SplitByDelimiter(beam.PTransform, beam.DoFn):
    def __init__(self, delimiter: str):
        self.delimiter = delimiter

    def process(self, element, delimiter: str):
        yield element.split(delimiter)

    def expand(self, pcoll):
        return (pcoll | beam.ParDo(self.process, delimiter=self.delimiter))


class FormatToDict(beam.PTransform, beam.DoFn):
    def __init__(self, field_names: list):
        self.field_names = field_names
    
    def process(self, element, field_names: list):
        yield dict(zip(field_names, [*element]))
        
    def expand(self, pcoll: beam.PCollection):
        return (pcoll | beam.ParDo(self.process, field_names=self.field_names))


class SplitToDict(beam.PTransform, beam.DoFn):
    def __init__(self, field_names: list, delimiter: str):
        self.field_names = field_names
        self.delimiter = delimiter
    
    def process(self, element: str, field_names: list, delimiter: str):
        yield dict(zip(field_names, element.split(delimiter)))
        
    def expand(self, pcoll: beam.PCollection):
        return (pcoll | beam.ParDo(self.process, field_names=self.field_names, delimiter=self.delimiter))


class Dtype_Transform(beam.PTransform, beam.DoFn):
    def __init__(self, schema: str):
        self.schema = schema
    
    def process(self, data, schema):
        strings,ints,floats,dates = [],[],[],[]
        for column in schema.split(','):
            col_name = column.split(':')[0]
            col_dtype = column.split(':')[1]
            if col_dtype == 'STRING':
                strings.append(col_name)
            elif col_dtype == 'INTEGER':
                ints.append(col_name)
            elif col_dtype == 'FLOAT':
                floats.append(col_name)
            elif col_dtype == 'DATETIME':
                dates.append(col_name)
        type_dic = {
            'strings': strings,
            'ints': ints,
            'floats': floats,
            'dates': dates
        }
        dates,ints,floats,strings = list(type_dic['dates']), list(type_dic['ints']), list(type_dic['floats']), list(type_dic['strings'])
        for key in data.keys():
            if key in dates:
                try:
                    data[key] = datetime.strptime(data[key], '%Y-%m-%d %H:%M:%S')
                except:
                    data[key] = None
            elif key in ints:
                try:
                    data[key] = int(data[key])
                except:
                    data[key] = None
            elif key in floats:
                try:
                    data[key] = float(data[key])
                except:
                    data[key] = None
            elif key in strings:
                if data[key] == '':
                    data[key] = None
        return [data]
    
    def expand(self, pcoll: beam.PCollection):
        return (pcoll | beam.ParDo(self.process, schema=self.schema))


class InnerJoin(beam.PTransform, beam.DoFn):
    def __init__(self, join_keys: dict, left_pcoll_name: str, right_pcoll_name: str):
        self.join_keys = join_keys
        self.left_pcoll_name = left_pcoll_name
        self.right_pcoll_name = right_pcoll_name
        
        if not isinstance(self.join_keys, dict):
            raise TypeError("Parameter `join_keys` must be of type: dict")
        if not isinstance(self.left_pcoll_name, str):
            raise TypeError("Parameter `left_pcoll_name` must be of type: str")   
        if not isinstance(self.right_pcoll_name, str):
                    raise TypeError("Parameter `right_pcoll_name` must be of type: str")   
            
        
    def process(self, element, left_pcoll_name, right_pcoll_name):
        group_key, grouped_dict = element
        join_dictionaries = grouped_dict[right_pcoll_name]
        source_dictionaries = grouped_dict[left_pcoll_name]
        if not source_dictionaries or not join_dictionaries:
            pass
        else:
            for source_dictionary in source_dictionaries:
                for join_dictionary in join_dictionaries:
                    source_dictionary.update(join_dictionary)
                    yield source_dictionary
        
    def expand(self, pcolls):
        def _format_as_common_key_tuple(data_dict, join_keys):
            return [data_dict[key] for key in join_keys], data_dict
        
        return ({pcoll_name: pcoll
                | f'Convert to ([join_keys], elem) for {pcoll_name}'
                    >> beam.Map(_format_as_common_key_tuple, self.join_keys[pcoll_name]) for (pcoll_name, pcoll) in pcolls.items()}
                | f'CoGroupByKey {pcolls.keys()}' >> beam.CoGroupByKey()
                | 'Inner Join Rows' >> beam.ParDo(self.process, left_pcoll_name=self.left_pcoll_name, right_pcoll_name=self.right_pcoll_name))
   