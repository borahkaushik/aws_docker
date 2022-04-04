from ml_utils.date_utils import get_date_attributes
from datetime import datetime
import awswrangler as wr

class AthenaTask:

    def __init__(self,
                segment: str,
                start_idx: int,
                end_idx: int,
                day: str = None,
                month: str = None,
                year: str = None) :
        self.segment = segment
        self.start_idx = int(start_idx)
        self.end_idx = int(end_idx)
        curr_year, curr_month, curr_day = get_date_attributes(datetime.now())
        self.day = day if day is not None else curr_day
        self.month = month if month is not None else curr_month
        self.year = year if year is not None else curr_year

    def fetch_matches(self, batch_size:int = 7500):
        #self.day = '26'
        query = """
            select uuid1.uuid as uuid_a, uuid2.uuid as uuid_b, input1.input_ids as input_a, input2.input_ids as input_b
                from (select distinct id_a as id_a, id_b as id_b from type_id_similar_matches 
                        where  day = '{0}' and month = '{1}' and year = '{2}' and domain = '{5}' offset {3} limit {4}) as match, 
                    type_id_uuid as uuid1, type_id_uuid as uuid2, type_id_input_id input1, type_id_input_id input2
            where match.id_a = uuid1.id and match.id_b = uuid2.id and uuid1.id = input1.id and uuid2.id = input2.id 
                and uuid1.day = '{0}' and uuid1.month = '{1}' and uuid1.year = '{2}' and uuid1.domain = '{5}'
                and uuid2.day = '{0}' and uuid2.month = '{1}' and uuid2.year = '{2}' and uuid2.domain = '{5}'
                and input1.day = '{0}' and input1.month = '{1}' and input1.year = '{2}' and input1.domain = '{5}'
                and input2.day = '{0}' and input2.month = '{1}' and input2.year = '{2}' and input2.domain = '{5}'
                and split_part(uuid1.uuid,'<>',3) != split_part(uuid2.uuid,'<>',3) 
                and split_part(uuid1.uuid,'<>',4) != split_part(uuid2.uuid,'<>',4)
        """.format(self.day, self.month, self.year, self.start_idx, self.end_idx - self.start_idx, self.segment)
        # query = """select uuid_a, uuid_b, input_a, input_b from (select distinct uuid1.uuid as uuid_a, uuid2.uuid as uuid_b, 
        #         t3.input_ids as input_a, t4.input_ids as input_b 
        #         from new_stack as match, new_stack_id_uuid as uuid1, new_stack_id_uuid as uuid2, in_input_new as t3, in_input_new as t4
        #         where match.id_a = uuid1.id and match.id_b = uuid2.id and t3.id = uuid1.id and t4.id = uuid2.id)
        #         where split_part(uuid_a,'<>',3) != split_part(uuid_b,'<>',3) and split_part(uuid_a,'<>',4) != split_part(uuid_b,'<>',4)
        #         order by uuid_a asc offset {0} limit {1}""".format(self.start_idx, self.end_idx - self.start_idx)
        print(query)
        df_iter = wr.athena.read_sql_query(
            sql=query,
            database="ml_internal",
            ctas_approach=True,
            chunksize=int(batch_size),
            keep_files=False
        )
        return df_iter

    
    