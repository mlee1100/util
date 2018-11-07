import json
import smart_open
import pprint
# from multiprocessing import Pool


class SchemaCompiler(object):

    def compile_from_jl_file(self, path, lines=None):
        schema = dict()
        with smart_open.smart_open(path, 'rb') as ifile:
            for i, line in enumerate(ifile):
                schema = self.merge(schema, self._flatten(self._schematize(json.loads(line))))
                if i == lines:
                    break
        return schema

    def _schematize(self, obj_):
        if not obj_:
            return obj_
        elif type(obj_) is dict:
            return {k: self._schematize(v) for k, v in obj_.iteritems()}
        elif type(obj_) is list:
            flattened = [self._schematize(v) for v in obj_]
            return [i for n, i in enumerate(flattened) if i not in flattened[n + 1:]]
        else:
            return type(obj_)()

    def _flatten(
        self,
        d,
        path=(),
    ):
        # create fill rate counting dict
        result = dict()
        result[path] = str(type(d))
        # if not isinstance(d, (dict, list, tuple)):
        #     if d is not None and str(d).strip() != '':
        #         result[path] += 1
        if isinstance(d, dict):
            for k, v in d.items():
                result.update(self._flatten(v, path + (k,)))
        elif isinstance(d, (list, tuple)):
            if d:
                if isinstance(d[0], (dict, list)):
                    for v in d:
                        result.update(self._flatten(v, path + (0,)))
                # else:
                #     if d[0] is not None and str(d[0]).strip() != '':
                #         result[path] += 1
        return result

    # def _reshape(self, d):
    #     o = dict()
    #     max_nested_level = len(sorted(d, key=len, reverse=True)[0])
    #     for key_tuple in sorted(d, key=len):
    #         ptype = d[key_tuple]
    #         for level in xrange(1, max_nested_level):
    #             if len(d) == level:




    def merge(self, *dicts):
        o = dict()
        map(o.update, dicts)
        return o





def test():
    sc = SchemaCompiler()
    files = [
        's3://nwd-imports/localblox/consumer/2018-10-30/Recrawled_20m_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/final_people_data_2017_05_26_48m_foundsource_v15_out_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/final_people_data_2017_05_26_48m_notfoundsource_v15_out_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20170717_massformat_part6_v20170914_oth_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20170717_massformat_part7_v15_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20171108_massformat_part8_from_new_source_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20171208_massformat_part9_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20180106_massformat_part10_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20180416_massformat_part11_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/mergedDumps_20180522_massformat_part12_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people01_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people02_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people03_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people04_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people05_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people06_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people07_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people08_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people09_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people10_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people11_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people12_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people13_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people14_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people15_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people16_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people17_234.7m_u_v15_with_400m_ids.json.gz_custom.gz',
        's3://nwd-imports/localblox/consumer/2018-10-30/people_83.5m_d_v15_with_400m_ids.json.gz_custom.gz',
        ]
    ind_schema = [sc.compile_from_jl_file(file, 10000) for file in files]
    for ind_schem in ind_schema:
        assert ind_schema[0] == ind_schem, 'oh shit, \n{}\n{}'.format(ind_schema[0], ind_schem)
    # pprint.pprint(sc.merge(*ind_schema))


if __name__ == '__main__':
    test()